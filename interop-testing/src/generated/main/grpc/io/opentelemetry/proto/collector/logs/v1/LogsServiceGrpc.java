package io.opentelemetry.proto.collector.logs.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * Service that can be used to push logs between one Application instrumented with
 * OpenTelemetry and an collector, or between an collector and a central collector (in this
 * case logs are sent/received to/from multiple Applications).
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class LogsServiceGrpc {

  private LogsServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "opentelemetry.proto.collector.logs.v1.LogsService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest,
      io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse> getExportMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Export",
      requestType = io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest.class,
      responseType = io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest,
      io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse> getExportMethod() {
    io.grpc.MethodDescriptor<io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest, io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse> getExportMethod;
    if ((getExportMethod = LogsServiceGrpc.getExportMethod) == null) {
      synchronized (LogsServiceGrpc.class) {
        if ((getExportMethod = LogsServiceGrpc.getExportMethod) == null) {
          LogsServiceGrpc.getExportMethod = getExportMethod =
              io.grpc.MethodDescriptor.<io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest, io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Export"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new LogsServiceMethodDescriptorSupplier("Export"))
              .build();
        }
      }
    }
    return getExportMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static LogsServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<LogsServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<LogsServiceStub>() {
        @java.lang.Override
        public LogsServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new LogsServiceStub(channel, callOptions);
        }
      };
    return LogsServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static LogsServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<LogsServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<LogsServiceBlockingV2Stub>() {
        @java.lang.Override
        public LogsServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new LogsServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return LogsServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static LogsServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<LogsServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<LogsServiceBlockingStub>() {
        @java.lang.Override
        public LogsServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new LogsServiceBlockingStub(channel, callOptions);
        }
      };
    return LogsServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static LogsServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<LogsServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<LogsServiceFutureStub>() {
        @java.lang.Override
        public LogsServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new LogsServiceFutureStub(channel, callOptions);
        }
      };
    return LogsServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Service that can be used to push logs between one Application instrumented with
   * OpenTelemetry and an collector, or between an collector and a central collector (in this
   * case logs are sent/received to/from multiple Applications).
   * </pre>
   */
  public interface AsyncService {

    /**
     */
    default void export(io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest request,
        io.grpc.stub.StreamObserver<io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExportMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service LogsService.
   * <pre>
   * Service that can be used to push logs between one Application instrumented with
   * OpenTelemetry and an collector, or between an collector and a central collector (in this
   * case logs are sent/received to/from multiple Applications).
   * </pre>
   */
  public static abstract class LogsServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return LogsServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service LogsService.
   * <pre>
   * Service that can be used to push logs between one Application instrumented with
   * OpenTelemetry and an collector, or between an collector and a central collector (in this
   * case logs are sent/received to/from multiple Applications).
   * </pre>
   */
  public static final class LogsServiceStub
      extends io.grpc.stub.AbstractAsyncStub<LogsServiceStub> {
    private LogsServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LogsServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new LogsServiceStub(channel, callOptions);
    }

    /**
     */
    public void export(io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest request,
        io.grpc.stub.StreamObserver<io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getExportMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service LogsService.
   * <pre>
   * Service that can be used to push logs between one Application instrumented with
   * OpenTelemetry and an collector, or between an collector and a central collector (in this
   * case logs are sent/received to/from multiple Applications).
   * </pre>
   */
  public static final class LogsServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<LogsServiceBlockingV2Stub> {
    private LogsServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LogsServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new LogsServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     */
    public io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse export(io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getExportMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service LogsService.
   * <pre>
   * Service that can be used to push logs between one Application instrumented with
   * OpenTelemetry and an collector, or between an collector and a central collector (in this
   * case logs are sent/received to/from multiple Applications).
   * </pre>
   */
  public static final class LogsServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<LogsServiceBlockingStub> {
    private LogsServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LogsServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new LogsServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse export(io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getExportMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service LogsService.
   * <pre>
   * Service that can be used to push logs between one Application instrumented with
   * OpenTelemetry and an collector, or between an collector and a central collector (in this
   * case logs are sent/received to/from multiple Applications).
   * </pre>
   */
  public static final class LogsServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<LogsServiceFutureStub> {
    private LogsServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LogsServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new LogsServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse> export(
        io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getExportMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_EXPORT = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_EXPORT:
          serviceImpl.export((io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest) request,
              (io.grpc.stub.StreamObserver<io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getExportMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest,
              io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse>(
                service, METHODID_EXPORT)))
        .build();
  }

  private static abstract class LogsServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    LogsServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.opentelemetry.proto.collector.logs.v1.LogsServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("LogsService");
    }
  }

  private static final class LogsServiceFileDescriptorSupplier
      extends LogsServiceBaseDescriptorSupplier {
    LogsServiceFileDescriptorSupplier() {}
  }

  private static final class LogsServiceMethodDescriptorSupplier
      extends LogsServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    LogsServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (LogsServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new LogsServiceFileDescriptorSupplier())
              .addMethod(getExportMethod())
              .build();
        }
      }
    }
    return result;
  }
}
