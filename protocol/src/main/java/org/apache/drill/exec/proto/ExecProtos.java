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
// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: ExecutionProtos.proto

package org.apache.drill.exec.proto;

public final class ExecProtos {
  private ExecProtos() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface FragmentHandleOrBuilder extends
      // @@protoc_insertion_point(interface_extends:exec.bit.FragmentHandle)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>optional .exec.shared.QueryId query_id = 1;</code>
     */
    boolean hasQueryId();
    /**
     * <code>optional .exec.shared.QueryId query_id = 1;</code>
     */
    org.apache.drill.exec.proto.UserBitShared.QueryId getQueryId();
    /**
     * <code>optional .exec.shared.QueryId query_id = 1;</code>
     */
    org.apache.drill.exec.proto.UserBitShared.QueryIdOrBuilder getQueryIdOrBuilder();

    /**
     * <code>optional int32 major_fragment_id = 2;</code>
     */
    boolean hasMajorFragmentId();
    /**
     * <code>optional int32 major_fragment_id = 2;</code>
     */
    int getMajorFragmentId();

    /**
     * <code>optional int32 minor_fragment_id = 3;</code>
     */
    boolean hasMinorFragmentId();
    /**
     * <code>optional int32 minor_fragment_id = 3;</code>
     */
    int getMinorFragmentId();
  }
  /**
   * Protobuf type {@code exec.bit.FragmentHandle}
   */
  public static final class FragmentHandle extends
      com.google.protobuf.GeneratedMessage implements
      // @@protoc_insertion_point(message_implements:exec.bit.FragmentHandle)
      FragmentHandleOrBuilder {
    // Use FragmentHandle.newBuilder() to construct.
    private FragmentHandle(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private FragmentHandle(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final FragmentHandle defaultInstance;
    public static FragmentHandle getDefaultInstance() {
      return defaultInstance;
    }

    public FragmentHandle getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private FragmentHandle(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              org.apache.drill.exec.proto.UserBitShared.QueryId.Builder subBuilder = null;
              if (((bitField0_ & 0x00000001) == 0x00000001)) {
                subBuilder = queryId_.toBuilder();
              }
              queryId_ = input.readMessage(org.apache.drill.exec.proto.UserBitShared.QueryId.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(queryId_);
                queryId_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000001;
              break;
            }
            case 16: {
              bitField0_ |= 0x00000002;
              majorFragmentId_ = input.readInt32();
              break;
            }
            case 24: {
              bitField0_ |= 0x00000004;
              minorFragmentId_ = input.readInt32();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.apache.drill.exec.proto.ExecProtos.internal_static_exec_bit_FragmentHandle_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.drill.exec.proto.ExecProtos.internal_static_exec_bit_FragmentHandle_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.drill.exec.proto.ExecProtos.FragmentHandle.class, org.apache.drill.exec.proto.ExecProtos.FragmentHandle.Builder.class);
    }

    public static com.google.protobuf.Parser<FragmentHandle> PARSER =
        new com.google.protobuf.AbstractParser<FragmentHandle>() {
      public FragmentHandle parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new FragmentHandle(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<FragmentHandle> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    public static final int QUERY_ID_FIELD_NUMBER = 1;
    private org.apache.drill.exec.proto.UserBitShared.QueryId queryId_;
    /**
     * <code>optional .exec.shared.QueryId query_id = 1;</code>
     */
    public boolean hasQueryId() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional .exec.shared.QueryId query_id = 1;</code>
     */
    public org.apache.drill.exec.proto.UserBitShared.QueryId getQueryId() {
      return queryId_;
    }
    /**
     * <code>optional .exec.shared.QueryId query_id = 1;</code>
     */
    public org.apache.drill.exec.proto.UserBitShared.QueryIdOrBuilder getQueryIdOrBuilder() {
      return queryId_;
    }

    public static final int MAJOR_FRAGMENT_ID_FIELD_NUMBER = 2;
    private int majorFragmentId_;
    /**
     * <code>optional int32 major_fragment_id = 2;</code>
     */
    public boolean hasMajorFragmentId() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional int32 major_fragment_id = 2;</code>
     */
    public int getMajorFragmentId() {
      return majorFragmentId_;
    }

    public static final int MINOR_FRAGMENT_ID_FIELD_NUMBER = 3;
    private int minorFragmentId_;
    /**
     * <code>optional int32 minor_fragment_id = 3;</code>
     */
    public boolean hasMinorFragmentId() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional int32 minor_fragment_id = 3;</code>
     */
    public int getMinorFragmentId() {
      return minorFragmentId_;
    }

    private void initFields() {
      queryId_ = org.apache.drill.exec.proto.UserBitShared.QueryId.getDefaultInstance();
      majorFragmentId_ = 0;
      minorFragmentId_ = 0;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeMessage(1, queryId_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeInt32(2, majorFragmentId_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeInt32(3, minorFragmentId_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(1, queryId_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(2, majorFragmentId_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(3, minorFragmentId_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static org.apache.drill.exec.proto.ExecProtos.FragmentHandle parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.drill.exec.proto.ExecProtos.FragmentHandle parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.drill.exec.proto.ExecProtos.FragmentHandle parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.drill.exec.proto.ExecProtos.FragmentHandle parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.drill.exec.proto.ExecProtos.FragmentHandle parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static org.apache.drill.exec.proto.ExecProtos.FragmentHandle parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static org.apache.drill.exec.proto.ExecProtos.FragmentHandle parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static org.apache.drill.exec.proto.ExecProtos.FragmentHandle parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static org.apache.drill.exec.proto.ExecProtos.FragmentHandle parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static org.apache.drill.exec.proto.ExecProtos.FragmentHandle parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(org.apache.drill.exec.proto.ExecProtos.FragmentHandle prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code exec.bit.FragmentHandle}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:exec.bit.FragmentHandle)
        org.apache.drill.exec.proto.ExecProtos.FragmentHandleOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.drill.exec.proto.ExecProtos.internal_static_exec_bit_FragmentHandle_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.drill.exec.proto.ExecProtos.internal_static_exec_bit_FragmentHandle_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.drill.exec.proto.ExecProtos.FragmentHandle.class, org.apache.drill.exec.proto.ExecProtos.FragmentHandle.Builder.class);
      }

      // Construct using org.apache.drill.exec.proto.ExecProtos.FragmentHandle.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
          getQueryIdFieldBuilder();
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        if (queryIdBuilder_ == null) {
          queryId_ = org.apache.drill.exec.proto.UserBitShared.QueryId.getDefaultInstance();
        } else {
          queryIdBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        majorFragmentId_ = 0;
        bitField0_ = (bitField0_ & ~0x00000002);
        minorFragmentId_ = 0;
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.drill.exec.proto.ExecProtos.internal_static_exec_bit_FragmentHandle_descriptor;
      }

      public org.apache.drill.exec.proto.ExecProtos.FragmentHandle getDefaultInstanceForType() {
        return org.apache.drill.exec.proto.ExecProtos.FragmentHandle.getDefaultInstance();
      }

      public org.apache.drill.exec.proto.ExecProtos.FragmentHandle build() {
        org.apache.drill.exec.proto.ExecProtos.FragmentHandle result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public org.apache.drill.exec.proto.ExecProtos.FragmentHandle buildPartial() {
        org.apache.drill.exec.proto.ExecProtos.FragmentHandle result = new org.apache.drill.exec.proto.ExecProtos.FragmentHandle(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        if (queryIdBuilder_ == null) {
          result.queryId_ = queryId_;
        } else {
          result.queryId_ = queryIdBuilder_.build();
        }
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.majorFragmentId_ = majorFragmentId_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.minorFragmentId_ = minorFragmentId_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof org.apache.drill.exec.proto.ExecProtos.FragmentHandle) {
          return mergeFrom((org.apache.drill.exec.proto.ExecProtos.FragmentHandle)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.drill.exec.proto.ExecProtos.FragmentHandle other) {
        if (other == org.apache.drill.exec.proto.ExecProtos.FragmentHandle.getDefaultInstance()) return this;
        if (other.hasQueryId()) {
          mergeQueryId(other.getQueryId());
        }
        if (other.hasMajorFragmentId()) {
          setMajorFragmentId(other.getMajorFragmentId());
        }
        if (other.hasMinorFragmentId()) {
          setMinorFragmentId(other.getMinorFragmentId());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.apache.drill.exec.proto.ExecProtos.FragmentHandle parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.drill.exec.proto.ExecProtos.FragmentHandle) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private org.apache.drill.exec.proto.UserBitShared.QueryId queryId_ = org.apache.drill.exec.proto.UserBitShared.QueryId.getDefaultInstance();
      private com.google.protobuf.SingleFieldBuilder<
          org.apache.drill.exec.proto.UserBitShared.QueryId, org.apache.drill.exec.proto.UserBitShared.QueryId.Builder, org.apache.drill.exec.proto.UserBitShared.QueryIdOrBuilder> queryIdBuilder_;
      /**
       * <code>optional .exec.shared.QueryId query_id = 1;</code>
       */
      public boolean hasQueryId() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>optional .exec.shared.QueryId query_id = 1;</code>
       */
      public org.apache.drill.exec.proto.UserBitShared.QueryId getQueryId() {
        if (queryIdBuilder_ == null) {
          return queryId_;
        } else {
          return queryIdBuilder_.getMessage();
        }
      }
      /**
       * <code>optional .exec.shared.QueryId query_id = 1;</code>
       */
      public Builder setQueryId(org.apache.drill.exec.proto.UserBitShared.QueryId value) {
        if (queryIdBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          queryId_ = value;
          onChanged();
        } else {
          queryIdBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>optional .exec.shared.QueryId query_id = 1;</code>
       */
      public Builder setQueryId(
          org.apache.drill.exec.proto.UserBitShared.QueryId.Builder builderForValue) {
        if (queryIdBuilder_ == null) {
          queryId_ = builderForValue.build();
          onChanged();
        } else {
          queryIdBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>optional .exec.shared.QueryId query_id = 1;</code>
       */
      public Builder mergeQueryId(org.apache.drill.exec.proto.UserBitShared.QueryId value) {
        if (queryIdBuilder_ == null) {
          if (((bitField0_ & 0x00000001) == 0x00000001) &&
              queryId_ != org.apache.drill.exec.proto.UserBitShared.QueryId.getDefaultInstance()) {
            queryId_ =
              org.apache.drill.exec.proto.UserBitShared.QueryId.newBuilder(queryId_).mergeFrom(value).buildPartial();
          } else {
            queryId_ = value;
          }
          onChanged();
        } else {
          queryIdBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>optional .exec.shared.QueryId query_id = 1;</code>
       */
      public Builder clearQueryId() {
        if (queryIdBuilder_ == null) {
          queryId_ = org.apache.drill.exec.proto.UserBitShared.QueryId.getDefaultInstance();
          onChanged();
        } else {
          queryIdBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }
      /**
       * <code>optional .exec.shared.QueryId query_id = 1;</code>
       */
      public org.apache.drill.exec.proto.UserBitShared.QueryId.Builder getQueryIdBuilder() {
        bitField0_ |= 0x00000001;
        onChanged();
        return getQueryIdFieldBuilder().getBuilder();
      }
      /**
       * <code>optional .exec.shared.QueryId query_id = 1;</code>
       */
      public org.apache.drill.exec.proto.UserBitShared.QueryIdOrBuilder getQueryIdOrBuilder() {
        if (queryIdBuilder_ != null) {
          return queryIdBuilder_.getMessageOrBuilder();
        } else {
          return queryId_;
        }
      }
      /**
       * <code>optional .exec.shared.QueryId query_id = 1;</code>
       */
      private com.google.protobuf.SingleFieldBuilder<
          org.apache.drill.exec.proto.UserBitShared.QueryId, org.apache.drill.exec.proto.UserBitShared.QueryId.Builder, org.apache.drill.exec.proto.UserBitShared.QueryIdOrBuilder> 
          getQueryIdFieldBuilder() {
        if (queryIdBuilder_ == null) {
          queryIdBuilder_ = new com.google.protobuf.SingleFieldBuilder<
              org.apache.drill.exec.proto.UserBitShared.QueryId, org.apache.drill.exec.proto.UserBitShared.QueryId.Builder, org.apache.drill.exec.proto.UserBitShared.QueryIdOrBuilder>(
                  getQueryId(),
                  getParentForChildren(),
                  isClean());
          queryId_ = null;
        }
        return queryIdBuilder_;
      }

      private int majorFragmentId_ ;
      /**
       * <code>optional int32 major_fragment_id = 2;</code>
       */
      public boolean hasMajorFragmentId() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>optional int32 major_fragment_id = 2;</code>
       */
      public int getMajorFragmentId() {
        return majorFragmentId_;
      }
      /**
       * <code>optional int32 major_fragment_id = 2;</code>
       */
      public Builder setMajorFragmentId(int value) {
        bitField0_ |= 0x00000002;
        majorFragmentId_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int32 major_fragment_id = 2;</code>
       */
      public Builder clearMajorFragmentId() {
        bitField0_ = (bitField0_ & ~0x00000002);
        majorFragmentId_ = 0;
        onChanged();
        return this;
      }

      private int minorFragmentId_ ;
      /**
       * <code>optional int32 minor_fragment_id = 3;</code>
       */
      public boolean hasMinorFragmentId() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>optional int32 minor_fragment_id = 3;</code>
       */
      public int getMinorFragmentId() {
        return minorFragmentId_;
      }
      /**
       * <code>optional int32 minor_fragment_id = 3;</code>
       */
      public Builder setMinorFragmentId(int value) {
        bitField0_ |= 0x00000004;
        minorFragmentId_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int32 minor_fragment_id = 3;</code>
       */
      public Builder clearMinorFragmentId() {
        bitField0_ = (bitField0_ & ~0x00000004);
        minorFragmentId_ = 0;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:exec.bit.FragmentHandle)
    }

    static {
      defaultInstance = new FragmentHandle(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:exec.bit.FragmentHandle)
  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_exec_bit_FragmentHandle_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_exec_bit_FragmentHandle_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\025ExecutionProtos.proto\022\010exec.bit\032\022Coord" +
      "ination.proto\032\023UserBitShared.proto\"n\n\016Fr" +
      "agmentHandle\022&\n\010query_id\030\001 \001(\0132\024.exec.sh" +
      "ared.QueryId\022\031\n\021major_fragment_id\030\002 \001(\005\022" +
      "\031\n\021minor_fragment_id\030\003 \001(\005B+\n\033org.apache" +
      ".drill.exec.protoB\nExecProtosH\001"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          org.apache.drill.exec.proto.CoordinationProtos.getDescriptor(),
          org.apache.drill.exec.proto.UserBitShared.getDescriptor(),
        }, assigner);
    internal_static_exec_bit_FragmentHandle_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_exec_bit_FragmentHandle_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_exec_bit_FragmentHandle_descriptor,
        new java.lang.String[] { "QueryId", "MajorFragmentId", "MinorFragmentId", });
    org.apache.drill.exec.proto.CoordinationProtos.getDescriptor();
    org.apache.drill.exec.proto.UserBitShared.getDescriptor();
  }

  // @@protoc_insertion_point(outer_class_scope)
}
