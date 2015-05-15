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
package org.apache.drill.exec.rpc;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.util.Version;
import io.netty.util.concurrent.ScheduledFuture;

import java.io.IOException;
import java.net.BindException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.GeneralRPCProtos.RpcMode;

import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

/**
 * A server is bound to a port and is responsible for responding to various type of requests. In some cases, the inbound
 * requests will generate more than one outbound request.
 */
public abstract class BasicServer<T extends EnumLite, C extends RemoteConnection> extends RpcBus<T, C> {
  //private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());

  protected static final String TIMEOUT_HANDLER = "timeout-handler";

  private ServerBootstrap b;
  private volatile boolean connect = false;
  private final EventLoopGroup eventLoopGroup;

  public BasicServer(final RpcConfig rpcMapping, ByteBufAllocator alloc, EventLoopGroup eventLoopGroup) {
    super(rpcMapping);
    this.eventLoopGroup = eventLoopGroup;

    b = new ServerBootstrap()
        .channel(TransportCheck.getServerSocketChannel())
        .option(ChannelOption.SO_BACKLOG, 1000)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30*1000)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.SO_RCVBUF, 1 << 17)
        .option(ChannelOption.SO_SNDBUF, 1 << 17)
        .group(eventLoopGroup) //
        .childOption(ChannelOption.ALLOCATOR, alloc)

        // .handler(new LoggingHandler(LogLevel.INFO))

        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
//            logger.debug("Starting initialization of server connection.");
            C connection = initRemoteConnection(ch);
            ch.closeFuture().addListener(getCloseHandler(ch, connection));

            final ChannelPipeline pipe = ch.pipeline();
            pipe.addLast("protocol-decoder", getDecoder(connection.getAllocator(), getOutOfMemoryHandler()));
            pipe.addLast("message-decoder", new RpcDecoder("s-" + rpcConfig.getName()));
            pipe.addLast("protocol-encoder", new RpcEncoder("s-" + rpcConfig.getName()));
            pipe.addLast("handshake-handler", getHandshakeHandler(connection));

            if (rpcMapping.hasTimeout()) {
              pipe.addLast(TIMEOUT_HANDLER,
                  new LogggingReadTimeoutHandler(connection, rpcMapping.getTimeout()));
              pipe.addFirst(WRITE_ACTIVITY_HANDLER, new WriteActivityHandler());
            }

            pipe.addLast("message-handler", new InboundHandler(connection));
            pipe.addLast("exception-handler", new RpcExceptionHandler(connection));

            connect = true;
//            logger.debug("Server connection initialization completed.");
          }
        });

//     if(TransportCheck.SUPPORTS_EPOLL){
//       b.option(EpollChannelOption.SO_REUSEPORT, true); //
//     }
  }

  /**
   * Replaces lastReadTime in the copy of Netty's ReadTimeoutHandler below. This
   * allows us to have write activity reset the timeout counter so that writing
   * large amounts of data to the pipe doesn't cause us to time out since we're
   * not receiving the heartbeats.
   */
  private volatile long lastActivityTime;

  private final static String WRITE_ACTIVITY_HANDLER = "write-activity-handler";

  private class WriteActivityHandler extends ChannelOutboundHandlerAdapter {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg,
        ChannelPromise promise) throws Exception {
      // Bump ahead the last activity time used for timeout purposes.
      lastActivityTime = System.nanoTime();
      ctx.write(msg, promise);
    }
  }

  private static final String NETTY_VERSION = "4.0.27.Final";

  /**
   * Raises a {@link ReadTimeoutException} when no data was read within a
   * certain period of time.
   *
   * <pre>
   * // The connection is closed when there is no inbound traffic
   * // for 30 seconds.
   *
   * public class MyChannelInitializer extends {@link ChannelInitializer}&lt;{@link Channel}&gt; {
   *     public void initChannel({@link Channel} channel) {
   *         channel.pipeline().addLast("readTimeoutHandler", new {@link NettyReadTimeoutHandler}(30);
   *         channel.pipeline().addLast("myHandler", new MyHandler());
   *     }
   * }
   *
   * // Handler should handle the {@link ReadTimeoutException}.
   * public class MyHandler extends {@link ChannelDuplexHandler} {
   *     {@code @Override}
   *     public void exceptionCaught({@link ChannelHandlerContext} ctx, {@link Throwable} cause)
   *             throws {@link Exception} {
   *         if (cause instanceof {@link ReadTimeoutException}) {
   *             // do something
   *         } else {
   *             super.exceptionCaught(ctx, cause);
   *         }
   *     }
   * }
   *
   * {@link ServerBootstrap} bootstrap = ...;
   * ...
   * bootstrap.childHandler(new MyChannelInitializer());
   * ...
   * </pre>
   *
   * @see WriteTimeoutHandler
   * @see IdleStateHandler
   */
  private static final long MIN_TIMEOUT_NANOS = TimeUnit.MILLISECONDS
      .toNanos(1); // Moved out of class.

  private class NettyReadTimeoutHandler extends ChannelInboundHandlerAdapter {
    private final long timeoutNanos;

    private volatile ScheduledFuture<?> timeout;
    // => lastActivityTime in the outer class
    // private volatile long lastReadTime;

    private volatile int state; // 0 - none, 1 - Initialized, 2 - Destroyed;

    private boolean closed;

    /**
     * Creates a new instance.
     *
     * @param timeoutSeconds
     *          read timeout in seconds
     */
    public NettyReadTimeoutHandler(int timeoutSeconds) {
      this(timeoutSeconds, TimeUnit.SECONDS);
    }

    /**
     * Creates a new instance.
     *
     * @param timeout
     *          read timeout
     * @param unit
     *          the {@link TimeUnit} of {@code timeout}
     */
    public NettyReadTimeoutHandler(long timeout, TimeUnit unit) {
      /*
       * if (!NETTY_VERSION.equals(nettyVersion.artifactVersion())) { throw new
       * IllegalStateException
       * (String.format("Netty artifact %s version %s did not match expected %s"
       * , artifactName, NETTY_VERSION, nettyVersion.artifactVersion())); }
       */
      boolean unexpectedNettyVersion = false;
      final Map<String, Version> nettyVersions = Version.identify();
      for (final String artifactName : nettyVersions.keySet()) {
        final Version nettyVersion = nettyVersions.get(artifactName);
        final String artifactVersion = nettyVersion.artifactVersion();

        if (!NETTY_VERSION.equals(artifactVersion)) {
          unexpectedNettyVersion = true;
          logger.warn(String.format(
              "nettyVersions[%s] => Version.artifactVersion() == %s",
              artifactName, nettyVersions.get(artifactName).artifactVersion()));
        }
      }

      if (unexpectedNettyVersion) {
        /*
         * If you get this exception, you've updated the version of Netty
         * without updating the code in here that we use. The
         * NettyReadTimeoutHandler class is a copy of Netty's ReadTimeoutHandler
         * class, but with the lastReadTime replaced with lastActivityTime in
         * this outer class so that we can make both reads and writes bump the
         * time ahead for the purposes of detecting time outs.
         *
         * Update the NettyReadTimeoutHandler class by copying Netty's version
         * of that class, and fixup the lastActivityTime reference. Then update
         * NETTY_VERSION appropriately based on the output in the log generated
         * above.
         */
        throw new IllegalStateException("Unexpected Netty version");
      }

      if (unit == null) {
        throw new NullPointerException("unit");
      }

      if (timeout <= 0) {
        timeoutNanos = 0;
      } else {
        timeoutNanos = Math.max(unit.toNanos(timeout), MIN_TIMEOUT_NANOS);
      }
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
      if (ctx.channel().isActive() && ctx.channel().isRegistered()) {
        // channelActvie() event has been fired already, which means
        // this.channelActive() will
        // not be invoked. We have to initialize here instead.
        initialize(ctx);
      } else {
        // channelActive() event has not been fired yet. this.channelActive()
        // will be invoked
        // and initialization will occur there.
      }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
      destroy();
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
      // Initialize early if channel is active already.
      if (ctx.channel().isActive()) {
        initialize(ctx);
      }
      super.channelRegistered(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      // This method will be invoked only if this handler was added
      // before channelActive() event is fired. If a user adds this handler
      // after the channelActive() event, initialize() will be called by
      // beforeAdd().
      initialize(ctx);
      super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      destroy();
      super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
        throws Exception {
      lastActivityTime = System.nanoTime();
      ctx.fireChannelRead(msg);
    }

    private void initialize(ChannelHandlerContext ctx) {
      // Avoid the case where destroy() is called before scheduling timeouts.
      // See: https://github.com/netty/netty/issues/143
      switch (state) {
      case 1:
      case 2:
        return;
      }

      state = 1;

      lastActivityTime = System.nanoTime();
      if (timeoutNanos > 0) {
        timeout = ctx.executor().schedule(new ReadTimeoutTask(ctx),
            timeoutNanos, TimeUnit.NANOSECONDS);
      }
    }

    private void destroy() {
      state = 2;

      if (timeout != null) {
        timeout.cancel(false);
        timeout = null;
      }
    }

    /**
     * Is called when a read timeout was detected.
     */
    protected void readTimedOut(ChannelHandlerContext ctx) throws Exception {
      if (!closed) {
        ctx.fireExceptionCaught(ReadTimeoutException.INSTANCE);
        ctx.close();
        closed = true;
      }
    }

    private final class ReadTimeoutTask implements Runnable {

      private final ChannelHandlerContext ctx;

      ReadTimeoutTask(ChannelHandlerContext ctx) {
        this.ctx = ctx;
      }

      @Override
      public void run() {
        if (!ctx.channel().isOpen()) {
          return;
        }

        long currentTime = System.nanoTime();
        long nextDelay = timeoutNanos - (currentTime - lastActivityTime);
        if (nextDelay <= 0) {
          // Read timed out - set a new timeout and notify the callback.
          timeout = ctx.executor().schedule(this, timeoutNanos,
              TimeUnit.NANOSECONDS);
          try {
            readTimedOut(ctx);
          } catch (Throwable t) {
            ctx.fireExceptionCaught(t);
          }
        } else {
          // Read occurred before the timeout - set a new timeout with shorter
          // delay.
          timeout = ctx.executor().schedule(this, nextDelay,
              TimeUnit.NANOSECONDS);
        }
      }
    }
  }

  private class LogggingReadTimeoutHandler<C extends RemoteConnection> extends NettyReadTimeoutHandler {

    private final C connection;
    private final int timeoutSeconds;
    public LogggingReadTimeoutHandler(C connection, int timeoutSeconds) {
      super(timeoutSeconds);
      this.connection = connection;
      this.timeoutSeconds = timeoutSeconds;
    }

    @Override
    protected void readTimedOut(ChannelHandlerContext ctx) throws Exception {
      logger.info("RPC connection {} timed out.  Timeout was set to {} seconds. Closing connection.", connection.getName(),
          timeoutSeconds);
      super.readTimedOut(ctx);
    }

  }

  public OutOfMemoryHandler getOutOfMemoryHandler() {
    return OutOfMemoryHandler.DEFAULT_INSTANCE;
  }

  protected void removeTimeoutHandler() {

  }

  public abstract ProtobufLengthDecoder getDecoder(BufferAllocator allocator, OutOfMemoryHandler outOfMemoryHandler);

  @Override
  public boolean isClient() {
    return false;
  }

  protected abstract ServerHandshakeHandler<?> getHandshakeHandler(C connection);

  protected static abstract class ServerHandshakeHandler<T extends MessageLite> extends AbstractHandshakeHandler<T> {

    public ServerHandshakeHandler(EnumLite handshakeType, Parser<T> parser) {
      super(handshakeType, parser);
    }

    @Override
    protected void consumeHandshake(ChannelHandlerContext ctx, T inbound) throws Exception {
      OutboundRpcMessage msg = new OutboundRpcMessage(RpcMode.RESPONSE, this.handshakeType, coordinationId,
          getHandshakeResponse(inbound));
      ctx.writeAndFlush(msg);
    }

    public abstract MessageLite getHandshakeResponse(T inbound) throws Exception;

  }

  @Override
  protected MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    return null;
  }

  @Override
  protected Response handle(C connection, int rpcType, ByteBuf pBody, ByteBuf dBody) throws RpcException {
    return null;
  }

  @Override
  public <SEND extends MessageLite, RECEIVE extends MessageLite> DrillRpcFuture<RECEIVE> send(C connection, T rpcType,
      SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) {
    return super.send(connection, rpcType, protobufBody, clazz, dataBodies);
  }

  @Override
  public <SEND extends MessageLite, RECEIVE extends MessageLite> void send(RpcOutcomeListener<RECEIVE> listener,
      C connection, T rpcType, SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) {
    super.send(listener, connection, rpcType, protobufBody, clazz, dataBodies);
  }

  @Override
  public C initRemoteConnection(SocketChannel channel) {
    local = channel.localAddress();
    remote = channel.remoteAddress();
    return null;
  }

  public int bind(final int initialPort, boolean allowPortHunting) throws DrillbitStartupException {
    int port = initialPort - 1;
    while (true) {
      try {
        b.bind(++port).sync();
        break;
      } catch (Exception e) {
        if (e instanceof BindException && allowPortHunting) {
          continue;
        }
        throw new DrillbitStartupException("Could not bind Drillbit", e);
      }
    }

    connect = !connect;
    logger.debug("Server started on port {} of type {} ", port, this.getClass().getSimpleName());
    return port;
  }

  @Override
  public void close() throws IOException {
    try {
      eventLoopGroup.shutdownGracefully().get();
    } catch (final InterruptedException | ExecutionException e) {
      logger.warn("Failure while shutting down {}. ", this.getClass().getName(), e);

      // Preserve evidence that the interruption occurred so that code higher up on the call stack can learn of the
      // interruption and respond to it if it wants to.
      Thread.currentThread().interrupt();
    }
  }

}
