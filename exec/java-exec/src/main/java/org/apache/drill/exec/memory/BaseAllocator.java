/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.memory;

import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.DrillBuf;
import io.netty.buffer.PooledByteBufAllocatorL;
import io.netty.buffer.UnsafeDirectLittleEndian;

import org.apache.drill.common.DrillAutoCloseables;
import org.apache.drill.common.StackTrace;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.util.AssertionUtil;
import org.apache.drill.exec.util.Pointer;

import com.google.common.base.Preconditions;

public abstract class BaseAllocator implements BufferAllocator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseAllocator.class);
  private static final AtomicInteger idGenerator = new AtomicInteger(0);
  private static final Object allocatorLock = new Object();

  private static final boolean DEBUG = AssertionUtil.isAssertionsEnabled()
      || Boolean.getBoolean(ExecConstants.DEBUG_ALLOCATOR);
  private static final PooledByteBufAllocatorL innerAllocator = PooledByteBufAllocatorL.DEFAULT;

  private long allocated; // the amount of memory this allocator has given out to its clients (including children)
  private long owned; // the amount of memory this allocator has obtained from its parent
  private long peakAllocated; // the most memory this allocator has given out during its lifetime
  private long bufferAllocation; // the amount of memory used just for directly allocated buffers, not children

  private boolean isClosed = false; // the allocator has been closed

  private final long maxAllocation; // the maximum amount of memory this allocator will give out
  private final AllocationPolicyAgent policyAgent;
  private final BaseAllocator parentAllocator;
  private final AllocatorOwner allocatorOwner;
  protected final int id = idGenerator.incrementAndGet(); // unique ID assigned to each allocator
  private final DrillBuf empty;
  private final AllocationPolicy allocationPolicy;
  private final InnerBufferLedger bufferLedger = new InnerBufferLedger();

  // members used purely for debugging
  private final IdentityHashMap<UnsafeDirectLittleEndian, BufferLedger> allocatedBuffers;
  private final IdentityHashMap<BaseAllocator, Object> childAllocators;

  private static class Event {
    private final String note;
    private final StackTrace stackTrace;

    public Event(final String note) {
      this.note = note;
      stackTrace = new StackTrace();
    }
  }

  private final List<Event> history;

  /**
   * Provide statistics via JMX for limiting root allocators.
   */
  private class RootAllocatorStats implements RootAllocatorStatsMXBean {
    @Override
    public long getOwnedMemory() {
      return owned;
    }

    @Override
    public long getAllocatedMemory() {
      return allocated;
    }

    @Override
    public long getChildCount() {
      if (childAllocators != null) {
        return childAllocators.size();
      }

      return -1; // unknown
    }
  }

  private static BaseAllocator getBaseAllocator(final BufferAllocator bufferAllocator) {
    if (!(bufferAllocator instanceof BaseAllocator)) {
      throw new IllegalArgumentException("expected a BaseAllocator instance, but got a "
          + bufferAllocator.getClass().getName());
    }
    return (BaseAllocator) bufferAllocator;
  }

  // TODO(cwestin) move allocation policy implementations to RootAllocator?
  private static class PerFragmentAllocationPolicy implements AllocationPolicy {
    static class Globals {
      private long maxBufferAllocation = 0;
      private final AtomicInteger limitingRoots = new AtomicInteger(0);
    }

    private final Globals globals = new Globals();

    @Override
    public AllocationPolicyAgent newAgent() {
      return new PerFragmentAllocationPolicyAgent(globals);
    }
  }

  /**
   * AllocationPolicy that allows each fragment running on a drillbit to share an
   * equal amount of direct memory, regardless of whether or not those fragments
   * belong to the same query.
   */
  public static final AllocationPolicy POLICY_PER_FRAGMENT = new PerFragmentAllocationPolicy();

  /**
   * String name of {@link #POLICY_PER_FRAGMENT} policy.
   */
  public static final String POLICY_PER_FRAGMENT_NAME = "per-fragment";

  private static class PerFragmentAllocationPolicyAgent implements AllocationPolicyAgent {
    private final PerFragmentAllocationPolicy.Globals globals;
    private boolean limitingRoot; // this is a limiting root; see F_LIMITING_ROOT

    // registered MBean object name (for JMX statistics reporting)
    private ObjectName objectName = null;

    PerFragmentAllocationPolicyAgent(PerFragmentAllocationPolicy.Globals globals) {
      this.globals = globals;
    }

    @Override
    public void close() throws Exception {
      if (limitingRoot) {
        // now there's one fewer active root
        final int rootCount = globals.limitingRoots.decrementAndGet();

        synchronized(globals) {
          /*
           * If the rootCount went to zero, we don't need to worry about setting the
           * maxBufferAllocation, because there aren't any allocators to reference it;
           * the next allocator to get created will set it appropriately.
           */
          if (rootCount != 0) {
            globals.maxBufferAllocation = RootAllocator.getMaxDirect() / rootCount;
          }
        }
      }

      if (objectName != null) {
        final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        mbs.unregisterMBean(objectName);
      }
    }

    @Override
    public void checkNewAllocator(BufferAllocator parentAllocator,
        long initReservation, long maxAllocation, int flags) {
/*
      Preconditions.checkArgument(parentAllocator != null, "parent allocator can't be null");
      Preconditions.checkArgument(parentAllocator instanceof BaseAllocator, "Parent allocator must be a BaseAllocator");
*/

//      final BaseAllocator baseAllocator = (BaseAllocator) parentAllocator;

      // this is synchronized to protect maxBufferAllocation
      synchronized(POLICY_PER_FRAGMENT) {
        // initialize maxBufferAllocation the very first time we call this
        if (globals.maxBufferAllocation == 0) {
          globals.maxBufferAllocation = RootAllocator.getMaxDirect();
        }

        if (limitingRoot = ((flags & F_LIMITING_ROOT) != 0)) {
          // figure out the new current per-allocator limit
          globals.maxBufferAllocation = RootAllocator.getMaxDirect() / (globals.limitingRoots.get() + 1);
        }

        if (initReservation > 0) {
          if (initReservation > globals.maxBufferAllocation) {
            throw new OutOfMemoryRuntimeException(
                String.format("can't fulfill initReservation request at this time "
                    + "(initReservation = %d > maxBufferAllocation = %d)",
                initReservation, globals.maxBufferAllocation));
          }
        }
      }
    }

    @Override
    public long getMemoryLimit(BufferAllocator bufferAllocator) {
      synchronized(POLICY_PER_FRAGMENT) {
        return globals.maxBufferAllocation;
      }
    }

    @Override
    public void initializeAllocator(final BufferAllocator bufferAllocator) {
      final BaseAllocator baseAllocator = getBaseAllocator(bufferAllocator);

      if (!limitingRoot) {
        objectName = null;
      } else {
        globals.limitingRoots.incrementAndGet();

        // publish management information for this allocator
        try {
          final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
          final RootAllocatorStatsMXBean mbean = baseAllocator.new RootAllocatorStats();
          objectName = new ObjectName("org.apache.drill.exec.memory:RootAllocator=" + baseAllocator.id);
          mbs.registerMBean(mbean, objectName);
        } catch(Exception e) {
          logger.warn("Exception setting up RootAllocatorStatsMXBean:\n" + e);
        }
      }
    }

    @Override
    public boolean shouldReleaseToParent(final BufferAllocator bufferAllocator) {
      final BaseAllocator baseAllocator = getBaseAllocator(bufferAllocator);
      return (baseAllocator.bufferAllocation > globals.maxBufferAllocation);
    }
  }

  private static class LocalMaxAllocationPolicy implements AllocationPolicy {
    // this agent is stateless, so we can always use the same one
    private static final AllocationPolicyAgent AGENT = new LocalMaxAllocationPolicyAgent();

    @Override
    public AllocationPolicyAgent newAgent() {
      return AGENT;
    }
  }

  /**
   * AllocationPolicy that imposes no limits on how much direct memory fragments
   * may allocate. LOCAL_MAX refers to the only limit that is enforced, which is
   * the maxAllocation specified at allocators' creation.
   *
   * <p>This policy ignores the value of {@link BufferAllocator#F_LIMITING_ROOT}.</p>
   */
  public static final AllocationPolicy POLICY_LOCAL_MAX = new LocalMaxAllocationPolicy();

  /**
   * String name of {@link #POLICY_LOCAL_MAX} allocation policy.
   */
  public static final String POLICY_LOCAL_MAX_NAME = "local-max";

  private static class LocalMaxAllocationPolicyAgent implements AllocationPolicyAgent {
    @Override
    public void close() throws Exception {
    }

    @Override
    public void checkNewAllocator(BufferAllocator parentAllocator,
        long initReservation, long maxAllocation, int flags) {
    }

    @Override
    public long getMemoryLimit(BufferAllocator bufferAllocator) {
      final BaseAllocator baseAllocator = (BaseAllocator) bufferAllocator;
      return baseAllocator.maxAllocation;
    }

    @Override
    public void initializeAllocator(BufferAllocator bufferAllocator) {
    }

    @Override
    public boolean shouldReleaseToParent(BufferAllocator bufferAllocator) {
      // since there are no shared limits, release space whenever we can
      return true;
    }
  }

  // TODO(DRILL-2698) POLICY_PER_QUERY

  protected BaseAllocator(final BaseAllocator parentAllocator,
      final AllocatorOwner allocatorOwner, final AllocationPolicy allocationPolicy,
      final long initReservation, final long maxAllocation,
      final int flags) throws OutOfMemoryRuntimeException {
    Preconditions.checkArgument(initReservation >= 0,
        "the initial reservation size must be non-negative");
    Preconditions.checkArgument(maxAllocation >= 0,
        "the maximum allocation limit mjst be non-negative");
    Preconditions.checkArgument(initReservation <= maxAllocation,
        "the initial reservation size must be <= the maximum allocation");

    if (initReservation > 0) {
      if (parentAllocator == null) {
        throw new IllegalStateException(
            "can't reserve memory without a parent allocator");
      }
    }

    // check to see if we can create this new allocator (the check throws if it's not ok)
    final AllocationPolicyAgent policyAgent = allocationPolicy.newAgent();
    policyAgent.checkNewAllocator(parentAllocator, initReservation, maxAllocation, flags);

    if ((initReservation > 0) && !parentAllocator.reserve(this, initReservation, 0)) {
      throw new OutOfMemoryRuntimeException(
          "can't fulfill initial reservation of size (unavailable from parent)" + initReservation);
    }

    this.parentAllocator = parentAllocator;
    this.allocatorOwner = allocatorOwner;
    this.allocationPolicy = allocationPolicy;
    this.policyAgent = policyAgent;
    this.maxAllocation = maxAllocation;

    // the root allocator owns all of its memory; anything else just owns it's initial reservation
    owned = parentAllocator == null ? maxAllocation : initReservation;
    empty = DrillBuf.getEmpty(new EmptyLedger(), this);

    if (DEBUG) {
      allocatedBuffers = new IdentityHashMap<>();
      childAllocators = new IdentityHashMap<>();
      history = new LinkedList<>();
      recordEvent("created with owned = %d", owned);
    } else {
      allocatedBuffers = null;
      childAllocators = null;
      history = null;
    }

    // now that we're not in danger of throwing an exception, we can take this step
    policyAgent.initializeAllocator(this);
  }

  private void recordEvent(final String noteFormat, Object... args) {
    final String note = String.format(noteFormat, args);
    history.add(new Event(note));
  }

  private void logEvents() {
    final StringWriter sw = new StringWriter();
    sw.write(String.format("\nallocator[%d] event log\n", id));
    for(final Event event : history) {
      sw.write("  ");
      sw.write(event.note);
      sw.write('\n');

      event.stackTrace.write(sw, 4);
    }
    logger.debug(sw.toString());
  }

  private StackTrace getCreationStack() {
    // The creation stack is always captured with the first event.
    final Event event = history.get(0);
    return event.stackTrace;
  }

  /**
   * Allocators without a parent must provide an implementation of this so
   * that they may reserve additional space even though they don't have a
   * parent they can fall back to.
   *
   * <p>Prior to calling this, BaseAllocator has verified that this won't violate
   * the maxAllocation for this allocator.</p>
   *
   * @param nBytes the amount of space to reserve
   * @param ignoreMax ignore the maximum allocation limit;
   *   see {@link ChildLedger#reserve(long, boolean)}.
   * @return true if the request can be met, false otherwise
   */
  protected boolean canIncreaseOwned(final long nBytes, final int flags) {
    if (parentAllocator == null) {
      return false;
    }

    return parentAllocator.reserve(this, nBytes, flags);
  }

  /**
   * Reserve space for the child allocator from this allocator.
   *
   * @param childAllocator child allocator making the request, or null
   *  if this is not for a child
   * @param nBytes how much to reserve
   * @param flags one or more of RESERVE_F_* flags or'ed together
   * @return true if the reservation can be satisfied, false otherwise
   */
  private static final int RESERVE_F_IGNORE_MAX = 0x0001;
  private boolean reserve(final BaseAllocator childAllocator,
      final long nBytes, final int flags) {
    Preconditions.checkArgument(nBytes >= 0,
        "the number of bytes to reserve must be non-negative");

    // we can always fulfill an empty request
    if (nBytes == 0) {
      return true;
    }

    final boolean ignoreMax = (flags & RESERVE_F_IGNORE_MAX) != 0;

    synchronized(allocatorLock) {
      if (isClosed) {
        resurrect();
      }

      final long ownAtLeast = allocated + nBytes;
      // Are we allowed to hand out this much?
      if (!ignoreMax && (ownAtLeast > maxAllocation)) {
        return false;
      }

      // do we need more from our parent first?
      if (ownAtLeast > owned) {
        final long needAdditional = ownAtLeast - owned;
        if (!canIncreaseOwned(needAdditional, flags)) {
          return false;
        }
        owned += needAdditional;

        if (DEBUG) {
          recordEvent("increased owned by %d, now owned == %d", needAdditional, owned);
        }
      }

      if (DEBUG) {
        if (owned < ownAtLeast) {
          throw new IllegalStateException("don't own enough memory to satisfy request");
        }
        if (allocated > owned) {
          throw new IllegalStateException(
              String.format("more memory allocated (%d) than owned (%d)", allocated, owned));
        }
      }

      allocated += nBytes;

      if (allocated > peakAllocated) {
        peakAllocated = allocated;
      }

      return true;
    }
  }

  private void releaseBytes(final long nBytes) {
    Preconditions.checkArgument(nBytes >= 0,
        "the number of bytes being released must be non-negative");

    synchronized(allocatorLock) {
      allocated -= nBytes;
    }
  }

  private void releaseBuffer(final DrillBuf drillBuf) {
    Preconditions.checkArgument(drillBuf != null,
        "the DrillBuf being released can't be null");

    final ByteBuf byteBuf = drillBuf.unwrap();
    final int udleMaxCapacity = byteBuf.maxCapacity();

    synchronized(allocatorLock) {
      final boolean releaseToParent = (parentAllocator != null)
          && policyAgent.shouldReleaseToParent(this);
      bufferAllocation -= udleMaxCapacity;
      releaseBytes(udleMaxCapacity);

      /*
       * Return space to our parent if our allocation is over the currently allowed amount.
       */
      if (releaseToParent) {
        final long canFree = owned - allocated;
        parentAllocator.releaseBytes(canFree);
        owned -= canFree;

        if (DEBUG) {
          recordEvent("returned %d to parent, now owned == %d", canFree, owned);
        }
      }

      if (DEBUG) {
        // make sure the buffer came from this allocator
        final Object object = allocatedBuffers.remove(byteBuf);
        if (object == null) {
          throw new IllegalStateException("Released buffer did not belong to this allocator");
        }
      }
    }
  }

  private void childClosed(final BaseAllocator childAllocator) {
    Preconditions.checkArgument(childAllocator != null, "child allocator can't be null");

    if (DEBUG) {
      synchronized(allocatorLock) {
        final Object object = childAllocators.remove(childAllocator);
        if (object == null) {
          throw new IllegalStateException("Child allocator[" + childAllocator.id
              + "] not found in parent allocator[" + id + "]'s childAllocators");
        }

        try {
          verifyAllocator();
        } catch(Exception e) {
          /*
           * If there was a problem with verification, the history of the closed
           * child may also be useful.
           */
          logger.debug("allocator[" + id + "]: exception while closing the following child");
          childAllocator.logEvents();

          // Continue with the verification exception throwing.
          throw e;
        }
      }
    }
  }

  /**
   * TODO(DRILL-2740) We use this to bypass the regular accounting for the
   * empty DrillBuf, because it is treated specially at this time. Once that
   * is remedied, this should be able to go away.
   */
  private class EmptyLedger implements BufferLedger {
    @Override
    public PooledByteBufAllocatorL getUnderlyingAllocator() {
      return innerAllocator;
    }

    @Override
    public void release(final DrillBuf drillBuf) {
      if (DEBUG) {
        if (drillBuf != empty) {
          throw new IllegalStateException("The empty buffer's ledger is being used to release something else");
        }
      }
    }

    @Override
    public BufferLedger shareWith(Pointer<DrillBuf> pDrillBuf,
        BufferLedger otherLedger, BufferAllocator otherAllocator, DrillBuf drillBuf,
        int index, int length, int drillBufFlags) {
      throw new UnsupportedOperationException("The empty buffer can't be shared");
    }

    @Override
    public boolean transferTo(BufferAllocator newAlloc,
        Pointer<BufferLedger> pNewLedger, DrillBuf drillBuf) {
      throw new UnsupportedOperationException("The empty buffer's ownership can't be changed");
    }
  }

  private class InnerBufferLedger implements BufferLedger {
    @Override
    public PooledByteBufAllocatorL getUnderlyingAllocator() {
      return innerAllocator;
    }

    @Override
    public void release(final DrillBuf drillBuf) {
      releaseBuffer(drillBuf);
    }

    @Override
    public BufferLedger shareWith(final Pointer<DrillBuf> pDrillBuf,
        final BufferLedger otherLedger, final BufferAllocator otherAllocator,
        final DrillBuf drillBuf, final int index, final int length, final int drillBufFlags) {
      final BaseAllocator baseAllocator = (BaseAllocator) otherAllocator;
      if (baseAllocator.isClosed) {
        baseAllocator.resurrect();
      }

      /*
       * If this is called, then the buffer isn't yet shared, and should
       * become so.
       */
      final SharedBufferLedger sharedLedger = new SharedBufferLedger(drillBuf, BaseAllocator.this);

      // Create the new wrapping drillbuf.
      final DrillBuf newBuf =
          new DrillBuf(sharedLedger, otherAllocator, drillBuf, index, length, drillBufFlags);
      sharedLedger.addMapping(newBuf, baseAllocator);

      if (DEBUG) {
        logger.debug(String.format("InnerBufferLedger.shareWith(..., otherAllocator[%d], drillBuf[%d], ...) at \n%s",
            baseAllocator.id, drillBuf.getId(), new StackTrace())); // TODO(cwestin)

        final BaseAllocator drillBufAllocator = (BaseAllocator) drillBuf.getAllocator();
        if (BaseAllocator.this != drillBufAllocator) {
          throw new IllegalStateException("DrillBuf's allocator(["
              + drillBufAllocator.id + "]) doesn't match this(["
              + BaseAllocator.this.id + "])");
        }

        // Replace the ledger for the existing buffer.
        final BufferLedger thisLedger = allocatedBuffers.put(
            (UnsafeDirectLittleEndian) drillBuf.unwrap(), sharedLedger);

        // If we throw any of these exceptions, we need to clean up newBuf.
        if (thisLedger == null) {
          DrillAutoCloseables.closeNoChecked(newBuf);
          throw new IllegalStateException("Shared buffer is unknown to the source allocator");
        }
        if (thisLedger != this) {
          DrillAutoCloseables.closeNoChecked(newBuf);
          throw new IllegalStateException("Buffer's ledger was not the one it should be");
        }
      }

      pDrillBuf.value = newBuf;
      return sharedLedger;
    }

    @Override
    public boolean transferTo(final BufferAllocator newAlloc,
        final Pointer<BufferLedger> pNewLedger, final DrillBuf drillBuf) {
      Preconditions.checkArgument(newAlloc != null, "New allocator cannot be null");
      Preconditions.checkArgument(newAlloc != BaseAllocator.this,
          "New allocator is same as current");
      Preconditions.checkArgument(newAlloc instanceof BaseAllocator,
          "New allocator isn't a BaseAllocator");
      Preconditions.checkArgument(pNewLedger.value != null, "Candidate new ledger can't be null");
      Preconditions.checkArgument(drillBuf != null, "DrillBuf can't be null");

      final BaseAllocator newAllocator = (BaseAllocator) newAlloc;
      if (newAllocator.isClosed) {
        newAllocator.resurrect();
      }

      return BaseAllocator.transferTo(newAllocator, pNewLedger.value, drillBuf);
    }
  }

  /**
   * Transfer ownership of a buffer from one allocator to another.
   *
   * <p>Assumes the allocatorLock is held.</p>
   *
   * @param newAllocator the new allocator
   * @param newLedger the new ledger to use (which could be shared)
   * @param drillBuf the buffer
   * @return true if the buffer's transfer didn't exceed the new owner's maximum
   *   allocation limit
   */
  private static boolean transferTo(final BaseAllocator newAllocator,
      final BufferLedger newLedger, final DrillBuf drillBuf) {
    final UnsafeDirectLittleEndian udle = (UnsafeDirectLittleEndian) drillBuf.unwrap();
    final int udleMaxCapacity = udle.maxCapacity();

    synchronized(allocatorLock) {
      // Account for the space and track the buffer.
      newAllocator.reserveForBuf(udleMaxCapacity);

      if (DEBUG) {
        final Object object = newAllocator.allocatedBuffers.put(udle, newLedger);
        if (object != null) {
          throw new IllegalStateException("Buffer unexpectedly found in new allocator");
        }
      }

      // Remove from the old allocator.
      final BaseAllocator oldAllocator = (BaseAllocator) drillBuf.getAllocator();
      oldAllocator.releaseBuffer(drillBuf);

      logger.debug(String.format("SharedBufferLedger.transferTo(otherAllocator[%d], ..., drillBuf[%d]) at\n%s",
          newAllocator.id, drillBuf.getId(), new StackTrace())); // TODO(cwestin)

      return newAllocator.allocated < newAllocator.maxAllocation;
    }
  }

  private static class SharedBufferLedger implements BufferLedger {
    private volatile BaseAllocator owningAllocator;
    private final IdentityHashMap<DrillBuf, BaseAllocator> bufferMap = new IdentityHashMap<>();

    public SharedBufferLedger(final DrillBuf drillBuf, final BaseAllocator baseAllocator) {
      addMapping(drillBuf, baseAllocator);
      owningAllocator = baseAllocator;
    }

    private void addMapping(final DrillBuf drillBuf, final BaseAllocator baseAllocator) {
      bufferMap.put(drillBuf, baseAllocator);
    }

    @Override
    public PooledByteBufAllocatorL getUnderlyingAllocator() {
      return innerAllocator;
    }

    @Override
    public void release(final DrillBuf drillBuf) {
      Preconditions.checkArgument(drillBuf != null, "drillBuf can't be null");

      /*
       * This is the only method on the shared ledger that can be entered without
       * having first come through an outside method on BaseAllocator (such
       * as takeOwnership() or shareOwnership()), all of which get the allocatorLock.
       * Operations in the below require the allocatorLock. We also need to synchronize
       * on this object to protect the bufferMap. In order to avoid a deadlock with other
       * methods, we have to get the allocatorLock first, as will be done in all the
       * other cases.
       */
      synchronized(allocatorLock) {
        synchronized(this) {
          final Object bufferObject = bufferMap.remove(drillBuf);
          if (DEBUG) {
            if (bufferObject == null) {
              throw new IllegalStateException("Buffer not found in SharedBufferLedger's buffer map");
            }
          }

          /*
           * If there are other buffers in the bufferMap that share this buffer's fate,
           * remove them, since they are also now invalid. As we examine buffers, take note
           * of any others that don't share this one's fate, but which belong to the same
           * allocator; if we find any such, then we can avoid transferring ownership at this
           * time.
           */
          final BaseAllocator bufferAllocator = (BaseAllocator) drillBuf.getAllocator();
          final List<DrillBuf> sameAllocatorSurvivors = new LinkedList<>();
          if (!bufferMap.isEmpty()) {
            /*
             * We're going to be modifying bufferMap (if we find any other related buffers);
             * in order to avoid getting a ConcurrentModificationException, we can't do it
             * on the same iteration we use to examine the buffers, so we use an intermediate
             * list to figure out which ones we have to remove.
             */
            final Set<Map.Entry<DrillBuf, BaseAllocator>> bufsToCheck = bufferMap.entrySet();
            final List<DrillBuf> sharedFateBuffers = new LinkedList<>();
            for(final Map.Entry<DrillBuf, BaseAllocator> mapEntry : bufsToCheck) {
              final DrillBuf otherBuf = mapEntry.getKey();
              if (otherBuf.hasSharedFate(drillBuf)) {
                sharedFateBuffers.add(otherBuf);
              } else {
                final BaseAllocator otherAllocator = mapEntry.getValue();
                if (otherAllocator == bufferAllocator) {
                  sameAllocatorSurvivors.add(otherBuf);
                }
              }
            }

            for(final DrillBuf bufToRemove : sharedFateBuffers) {
              bufferMap.remove(bufToRemove);
            }
          }

          if (sameAllocatorSurvivors.isEmpty()) {
            /*
             * If that was the owning allocator, then we need to transfer ownership to
             * another allocator (any one) that is part of the sharing set.
             *
             * When we release the buffer back to the allocator, release the root buffer,
             */
            if (bufferAllocator == owningAllocator) {
              if (bufferMap.isEmpty()) {
                /*
                 * There are no other allocators available to transfer to, so
                 * release the space to the owner.
                 */
                bufferAllocator.releaseBuffer(drillBuf);
              } else {
                // Pick another allocator, and transfer ownership to that.
                final Collection<BaseAllocator> allocators = bufferMap.values();
                final Iterator<BaseAllocator> allocatorIter = allocators.iterator();
                final BaseAllocator nextAllocator = allocatorIter.next();
                BaseAllocator.transferTo(nextAllocator, this, drillBuf);
                owningAllocator = nextAllocator;
              }
            }
          }
        }
      }
    }

    @Override
    public BufferLedger shareWith(final Pointer<DrillBuf> pDrillBuf,
        final BufferLedger otherLedger, final BufferAllocator otherAllocator,
        final DrillBuf drillBuf, final int index, final int length, final int drillBufFlags) {
      final BaseAllocator baseAllocator = (BaseAllocator) otherAllocator;
      if (baseAllocator.isClosed) {
        baseAllocator.resurrect();
      }

      /*
       * This buffer is already shared, but we want to add more sharers.
       *
       * Create the new wrapper.
       */
      final DrillBuf newBuf = new DrillBuf(this, otherAllocator, drillBuf, index, length, drillBufFlags);
      logger.debug(String.format("SharedBufferLedger.shareWith(..., otherAllocator[%d], drillBuf[%d], ...) at \n%s",
          baseAllocator.id, drillBuf.getId(), new StackTrace())); // TODO(cwestin)
      addMapping(newBuf, baseAllocator);
      pDrillBuf.value = newBuf;
      return this;
    }

    @Override
    public boolean transferTo(final BufferAllocator newAlloc,
        final Pointer<BufferLedger> pNewLedger, final DrillBuf drillBuf) {
      Preconditions.checkArgument(newAlloc != null, "New allocator cannot be null");
      Preconditions.checkArgument(newAlloc instanceof BaseAllocator,
          "New allocator isn't a BaseAllocator");
      Preconditions.checkArgument(pNewLedger.value != null, "Candidate new ledger can't be null");
      Preconditions.checkArgument(drillBuf != null, "DrillBuf can't be null");

      final BaseAllocator newAllocator = (BaseAllocator) newAlloc;
      if (newAllocator.isClosed) {
        newAllocator.resurrect();
      }

      synchronized(this) {
        // Modify the buffer mapping to reflect the virtual transfer.
        final BaseAllocator oldAllocator = bufferMap.put(drillBuf, newAllocator);
        if (oldAllocator == null) {
          throw new IllegalStateException("No previous entry in SharedBufferLedger for drillBuf");
        }

        // Whatever happens, this is the new ledger.
        pNewLedger.value = this;

        /*
         * If the oldAllocator was the owner, then transfer ownership to the new allocator.
         */
        if (oldAllocator == owningAllocator) {
          owningAllocator = newAllocator;
          return BaseAllocator.transferTo(newAllocator, this, drillBuf);
        }

        // Even though we didn't do a real transfer, tell if it would have fit the limit.
        final int udleMaxCapacity = drillBuf.unwrap().maxCapacity();
        return newAllocator.allocated + udleMaxCapacity < newAllocator.maxAllocation;
      }
    }
  }

  @Override
  public DrillBuf buffer(int size) {
    return buffer(size, size);
  }

  @Override
  public DrillBuf buffer(final int minSize, final int maxSize) {
    Preconditions.checkArgument(minSize >= 0,
        "the minimimum requested size must be non-negative");
    Preconditions.checkArgument(maxSize >= 0,
        "the maximum requested size must be non-negative");
    Preconditions.checkArgument(minSize <= maxSize,
        "the minimum requested size must be <= the maximum requested size");

    // we can always return an empty buffer
    if (minSize == 0) {
      return getEmpty();
    }

    synchronized(allocatorLock) {
      // Don't allow the allocation if it will take us over the limit.
      final long allocatedWas = allocated;
      if (!reserve(null, maxSize, 0)) {
        return null;
      }

      final long reserved = allocated - allocatedWas;
      assert reserved == maxSize;

      final UnsafeDirectLittleEndian buffer = innerAllocator.directBuffer(minSize, maxSize);
      final int actualSize = buffer.maxCapacity();
      if (actualSize > maxSize) {
        final int extraSize = actualSize - maxSize;
        reserve(null, extraSize, RESERVE_F_IGNORE_MAX);
      }

      final DrillBuf wrapped = new DrillBuf(bufferLedger, this, buffer);
      buffer.release(); // Should have been retained by the DrillBuf constructor.
      assert buffer.refCnt() == 1 : "buffer was not retained by DrillBuf";
      assert allocated <= owned : "allocated more memory than owned";

      bufferAllocation += maxSize;
      if (allocated > peakAllocated) {
        peakAllocated = allocated;
      }

      if (allocatedBuffers != null) {
        allocatedBuffers.put(buffer, bufferLedger);
      }

      return wrapped;
    }
  }

  @Override
  public ByteBufAllocator getUnderlyingAllocator() {
    return innerAllocator;
  }

  @Override
  public BufferAllocator newChildAllocator(final AllocatorOwner allocatorOwner,
      final long initReservation, final long maxAllocation, final int flags) {
    synchronized(allocatorLock) {
      final BaseAllocator childAllocator =
          new ChildAllocator(this, allocatorOwner, allocationPolicy,
              initReservation, maxAllocation, flags);

      if (DEBUG) {
        childAllocators.put(childAllocator, childAllocator);
        logger.debug(String.format("allocator[%d] created new child allocator[%d] at\n%s",
            id, childAllocator.id, new StackTrace())); // TODO(cwestin)
      }

      return childAllocator;
    }
  }

  @Override
  public BufferAllocator getChildAllocator(FragmentContext fragmentContext,
      final long initialReservation, final long maximumAllocation,
      final boolean applyFragmentLimit) {

/* TODO(cwestin)
    if (fragmentContext != null) {
      throw new IllegalArgumentException("fragmentContext is non-null");
    }

    if (!applyFragmentLimit) {
      throw new IllegalArgumentException("applyFragmentLimit is false");
    }
    */

    return newChildAllocator(allocatorOwner, initialReservation, maximumAllocation,
        (applyFragmentLimit ? F_LIMITING_ROOT : 0));
  }

  /**
   * Reserve space for a DrillBuf for an ownership transfer.
   *
   * @param drillBuf the buffer to reserve space for
   */
  private void reserveForBuf(final int maxCapacity) {
    final boolean reserved = reserve(null, maxCapacity, RESERVE_F_IGNORE_MAX);
    if (DEBUG) {
      if (!reserved) {
        throw new IllegalStateException("reserveForBuf() failed");
      }
    }
  }

  @Override
  public boolean takeOwnership(final DrillBuf drillBuf) {
    synchronized(allocatorLock) {
      return drillBuf.transferTo(this, bufferLedger);
    }
  }

  @Override
  public boolean shareOwnership(final DrillBuf drillBuf, final Pointer<DrillBuf> bufOut) {
    synchronized(allocatorLock) {
      bufOut.value = drillBuf.shareWith(bufferLedger, this, 0, drillBuf.capacity());
      return allocated < maxAllocation;
    }
  }

  /*
   * It's not clear why we'd allow anyone to set their own limit, need to see why this is used;
   * this also doesn't make sense when the limits are constantly shifting, nor for other
   * allocation policies.
   */
  @Deprecated
  @Override
  public void setFragmentLimit(long fragmentLimit) {
    throw new UnsupportedOperationException("unimplemented:BaseAllocator.setFragmentLimit()"); // TODO(cwestin)
  }

  /**
   * Get the fragment limit. This was originally meant to be the maximum amount
   * of memory the currently running fragment (which owns this allocator or
   * its ancestor) may use. Note that the value may vary up and down over time
   * as fragments come and go on the node.
   *
   * <p>This is deprecated because the concept is not entirely stable. This
   * only makes sense for one particular memory allocation policy, which is the
   * one that sets limits on what fragments on a node may use by dividing up all
   * the memory evenly between all the fragments (see {@see #POLICY_PER_FRAGMENT}).
   * Other allocation policies, such as the one that limits memory on a
   * per-query-per-node basis, wouldn't have a value for this. But we need to have
   * something until we figure out what to eplace this with because it is used by
   * some operators (such as ExternalSortBatch) to determine how much memory they
   * can use before they have to spill to disk.</p>
   *
   * @return the fragment limit
   */
  @Deprecated
  @Override
  public long getFragmentLimit() {
    return policyAgent.getMemoryLimit(this);
  }

  @Override
  public void close() throws Exception {
    synchronized(allocatorLock) {
      if (isClosed) {
        final String msg = String.format("Allocator has already been closed (owner=\"%s\")",
            allocatorOwner.toString());

        if (DEBUG) {
          throw new IllegalStateException(msg);
        } else {
          logger.warn(msg);
        }
      }

      if (DEBUG) {
        verifyAllocator();

        // are there outstanding child allocators?
        if (!childAllocators.isEmpty()) {
          logChildren();
          throw new IllegalStateException("Allocator closed with outstanding child allocators");
        }

        // are there outstanding buffers?
        final Set<UnsafeDirectLittleEndian> allocatedBuffers = this.allocatedBuffers.keySet();
        final int allocatedCount = allocatedBuffers.size();
        if (allocatedCount > 0) {
          logBuffers();
          throw new IllegalStateException(
              "Allocator closed with outstanding buffers allocated (" + allocatedCount + ")");
        }

        /* TODO(DRILL-2740)
        // We should be the only client holding a reference to empty now.
        final int emptyRefCnt = empty.refCnt();
        if (emptyRefCnt != 1) {
          final String msg = "empty buffer refCnt() == " + emptyRefCnt + " (!= 1)";
          final StringWriter stringWriter = new StringWriter();
          stringWriter.write(msg);
          stringWriter.write('\n');
          empty.writeState(stringWriter);
          logger.debug(stringWriter.toString());
          throw new IllegalStateException(msg);
        }
        */
      }

      // Is there unaccounted-for outstanding allocation?
      if (allocated > 0) {
        throw new IllegalStateException(
            String.format("Unaccounted for outstanding allocation (%d)", allocated));
      }

      // Let go of the empty buffer.
      empty.release(empty.refCnt());

      DrillAutoCloseables.closeNoChecked(policyAgent);

      // Inform our parent allocator that we've closed.
      if (parentAllocator != null) {
        parentAllocator.releaseBytes(owned);
        owned = 0; // See resurrect();
        parentAllocator.childClosed(this);
      }

      if (DEBUG) {
        recordEvent("closed");
      }

      isClosed = true;
    }
  }

  /**
   * Log information about child allocators; only works if DEBUG
   */
  private void logChildren() {
    logger.debug(String.format("allocator[%d] open child allocators START", id));
    final Set<BaseAllocator> allocators = childAllocators.keySet();
    for(final BaseAllocator childAllocator : allocators) {
      childAllocator.logEvents();
    }
    logger.debug(String.format("allocator[%d] open child allocators END", id));
  }

  private void logBuffers() {
    final StringWriter writer = new StringWriter();
    final Set<UnsafeDirectLittleEndian> udleSet = allocatedBuffers.keySet();

    writer.write("allocator[");
    writer.write(Integer.toString(id));
    writer.write("], ");
    writer.write(Integer.toString(udleSet.size()));
    writer.write(" allocated buffers\n");

    for(final UnsafeDirectLittleEndian udle : udleSet) {
      writer.write(udle.toString());
      writer.write("[identityHashCode == ");
      writer.write(Integer.toString(System.identityHashCode(udle)));
      writer.write("]\n");
    }

    logger.trace(writer.toString());
  }

  @Override
  public long getAllocatedMemory() {
    return allocated;
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public long getPeakMemoryAllocation() {
    return peakAllocated;
  }

  @Override
  public DrillBuf getEmpty() {
    empty.retain();
    // TODO(DRILL-2740) update allocatedBuffers
    return empty;
  }

  private class Reservation extends AllocationReservation {
    @Override
    protected boolean reserve(int nBytes) {
      return BaseAllocator.this.reserve(null, nBytes, 0);
    }

    @Override
    protected DrillBuf allocate(int nBytes) {
      /*
       * The reservation already added the requested bytes to the
       * allocators owned and allocated bytes via reserve(). This
       * ensures that they can't go away. But when we ask for the buffer
       * here, that will add to the allocated bytes as well, so we need to
       * return the same number back to avoid double-counting them.
       */
      synchronized(allocatorLock) {
        BaseAllocator.this.allocated -= nBytes;
        final DrillBuf drillBuf = BaseAllocator.this.buffer(nBytes);
        return drillBuf;
      }
    }

    @Override
    protected void releaseReservation(int nBytes) {
      releaseBytes(nBytes);
    }
  }

  @Override
  public AllocationReservation newReservation() {
    return new Reservation();
  }

  /**
   * Verifies the accounting state of the allocator. Only works for DEBUG.
   *
   * @throws IllegalStateException when any problems are found
   */
  protected void verifyAllocator() {
    final IdentityHashMap<UnsafeDirectLittleEndian, BaseAllocator> buffersSeen = new IdentityHashMap<>();
    verifyAllocator(buffersSeen);
  }

  /**
   * Verifies the accounting state of the allocator. Only works for DEBUG.
   *
   * <p>This overload is used for recursive calls, allowing for checking that DrillBufs are unique
   * across all allocators that are checked.</p>
   *
   * @param buffersSeen a map of buffers that have already been seen when walking a tree of allocators
   * @throws IllegalStateException when any problems are found
   */
  protected void verifyAllocator(
      final IdentityHashMap<UnsafeDirectLittleEndian, BaseAllocator> buffersSeen) {
    synchronized(allocatorLock) {
      // verify purely local accounting
      if (allocated > owned) {
        throw new IllegalStateException("Allocator (id = " + id + ") has allocated more than it owns");
      }

      // the empty buffer should still be empty
      final long emptyCapacity = empty.maxCapacity();
      if (emptyCapacity != 0) {
        throw new IllegalStateException("empty buffer maxCapacity() == " + emptyCapacity + " (!= 0)");
      }

      // The remaining tests can only be performed if we're in debug mode.
      if (!DEBUG) {
        return;
      }

      // verify my direct descendants
      final Set<BaseAllocator> childSet = childAllocators.keySet();
      for(final BaseAllocator childAllocator : childSet) {
        childAllocator.verifyAllocator(buffersSeen);
      }

      /*
       * Verify my relationships with my descendants.
       *
       * The sum of direct child allocators' owned memory must be <= my allocated memory;
       * my allocated memory also includes DrillBuf's directly allocated by me.
       */
      long childTotal = 0;
      for(final BaseAllocator childAllocator : childSet) {
        childTotal += childAllocator.owned;
      }
      if (childTotal > allocated) {
        logger.debug("allocator[" + id + "] child event logs START");
        for(final BaseAllocator childAllocator : childSet) {
          childAllocator.logEvents();
        }
        logger.debug("allocator[" + id + "] child event logs END");
        throw new IllegalStateException(
            "Child allocators own more memory (" + childTotal + ") than their parent (id = "
                + id + " ) has allocated (" + allocated + ')');
      }

      // Furthermore, the amount I've allocated should be that plus buffers I've allocated.
      long bufferTotal = 0;
      final Set<UnsafeDirectLittleEndian> udleSet = allocatedBuffers.keySet();
      for(final UnsafeDirectLittleEndian udle : udleSet) {
        /*
         * Even when shared, DrillBufs are rewrapped, so we should never see the same
         * instance twice.
         */
        final BaseAllocator otherOwner = buffersSeen.get(udle);
        if (otherOwner != null) {
          throw new IllegalStateException("This allocator's drillBuf already owned by another allocator");
        }
        buffersSeen.put(udle, this);

        bufferTotal += udle.maxCapacity();
      }

      if (bufferTotal + childTotal != allocated) {
        throw new IllegalStateException("buffer space (" + bufferTotal + ") + child space ("
            + childTotal + ") != allocated (" + allocated + ')');
      }
    }
  }

  public static boolean isDebug() {
    return DEBUG;
  }

  /**
   * Because of some crazy interdependent steps in fragment cleanup, it
   * is possible for an allocator to be used again after it has been closed if
   * an exception was thrown by another part of the fragment cleanup process.
   * If that happens, in order for accounting to work correctly, we have to
   * re-establish the link with it's parent. This will do that, and make
   * the allocator functional again.
   */
  private void resurrect() {
    if (!isClosed) {
      throw new IllegalStateException("allocator[" + id
          + "].resurrect() when it isn't closed");
    }

    /*
     * TODO
     * This feature looks too dangerous to use because allocators can be
     * resurrect()ed indirectly. In one case, it was to accomodate the
     * ownership transfer of an incoming buffer in DataServer.send(), but the
     * target allocator was closed as a result of it's owning fragment being
     * cancelled and closed at the time the data arrived. There would have been
     * no way to release the buffer or close the allocator again, which would
     * have been required in order to avoid memory leaks and keep memory
     * accounting straight.
     *
     * For now, we've disabled this by throwing this exception. If we discover
     * that we can indeed do without resurrect()able allocators, then we can
     * remove this method, and move the exception to some more
     * direct places, such as reserve(), and buffer(int, int).
     */
    throw new AllocatorClosedException("Attempt to use closed allocator[" + id + "]");

/* commented out to avoid unreachable code compilation error from the above
    if (parentAllocator != null) {
      if (parentAllocator.isClosed) {
        parentAllocator.resurrect();
      }

      if (DEBUG) {
        parentAllocator.childAllocators.put(this, this);
      }
    }

    isClosed = false;
    recordEvent("resurrect()ed");
*/
  }
}
