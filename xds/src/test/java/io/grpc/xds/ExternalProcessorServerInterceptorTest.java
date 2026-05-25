/*
 * Copyright 2024 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.xds;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.envoyproxy.envoy.config.core.v3.GrpcService;
import io.envoyproxy.envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor;
import io.envoyproxy.envoy.extensions.filters.http.ext_proc.v3.ProcessingMode;
import io.envoyproxy.envoy.service.ext_proc.v3.BodyMutation;
import io.envoyproxy.envoy.service.ext_proc.v3.BodyResponse;
import io.envoyproxy.envoy.service.ext_proc.v3.CommonResponse;
import io.envoyproxy.envoy.service.ext_proc.v3.ExternalProcessorGrpc;
import io.envoyproxy.envoy.service.ext_proc.v3.HeaderMutation;
import io.envoyproxy.envoy.service.ext_proc.v3.HeadersResponse;
import io.envoyproxy.envoy.service.ext_proc.v3.HttpBody;
import io.envoyproxy.envoy.service.ext_proc.v3.ProcessingRequest;
import io.envoyproxy.envoy.service.ext_proc.v3.ProcessingResponse;
import io.envoyproxy.envoy.service.ext_proc.v3.StreamedBodyResponse;
import io.envoyproxy.envoy.service.ext_proc.v3.TrailersResponse;
import io.grpc.Attributes;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ClientCall;

import io.grpc.Status;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServerInterceptors;
import io.grpc.stub.ServerCalls;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.internal.FakeClock;
import io.grpc.internal.GrpcUtil;
import io.grpc.util.MutableHandlerRegistry;


import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.xds.ExternalProcessorFilter.ExternalProcessorFilterConfig;
import io.grpc.xds.ExternalProcessorFilter.ExternalProcessorFilterOverrideConfig;

import io.grpc.xds.Filter.FilterContext;
import io.grpc.xds.client.Bootstrapper;
import io.grpc.xds.client.EnvoyProtoData.Node;
import io.grpc.xds.internal.grpcservice.CachedChannelManager;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.NameResolverRegistry;
import java.net.SocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.nio.charset.StandardCharsets;
import org.mockito.Mockito;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExternalProcessorServerInterceptorTest {
  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private String extProcServerName;
  private ExternalProcessorFilter.Provider provider;
  private static final FilterContext FAKE_CONTEXT = FilterContext.create(
      "test-filter", new io.grpc.MetricRecorder() {});
  private Filter.FilterConfigParseContext filterContext;
  private Bootstrapper.BootstrapInfo bootstrapInfo;
  private Bootstrapper.ServerInfo serverInfo;

  private static final MethodDescriptor<InputStream, InputStream> METHOD_SAY_HELLO_RAW =
      MethodDescriptor.<InputStream, InputStream>newBuilder()
          .setType(MethodDescriptor.MethodType.UNARY)
          .setFullMethodName("test.TestService/SayHello")
          .setRequestMarshaller(new MethodDescriptor.Marshaller<InputStream>() {
            @Override public InputStream stream(InputStream value) { return value; }
            @Override public InputStream parse(InputStream stream) { return stream; }
          })
          .setResponseMarshaller(new MethodDescriptor.Marshaller<InputStream>() {
            @Override public InputStream stream(InputStream value) { return value; }
            @Override public InputStream parse(InputStream stream) { return stream; }
          })
          .build();

  private static final MethodDescriptor<InputStream, InputStream> METHOD_SAY_HELLO_BIDI =
      MethodDescriptor.<InputStream, InputStream>newBuilder()
          .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
          .setFullMethodName("test.TestService/SayHelloBidi")
          .setRequestMarshaller(new MethodDescriptor.Marshaller<InputStream>() {
            @Override public InputStream stream(InputStream value) { return value; }
            @Override public InputStream parse(InputStream stream) { return stream; }
          })
          .setResponseMarshaller(new MethodDescriptor.Marshaller<InputStream>() {
            @Override public InputStream stream(InputStream value) { return value; }
            @Override public InputStream parse(InputStream stream) { return stream; }
          })
          .build();

  private static class ConcurrencyDetectingServerCall extends io.grpc.ForwardingServerCall.SimpleForwardingServerCall<InputStream, InputStream> {
    private final AtomicInteger activeCalls = new AtomicInteger(0);
    private final AtomicBoolean concurrentCallDetected = new AtomicBoolean(false);

    ConcurrencyDetectingServerCall(ServerCall<InputStream, InputStream> delegate) {
      super(delegate);
    }

    @Override
    public void sendMessage(InputStream message) {
      int active = activeCalls.incrementAndGet();
      if (active > 1) {
        concurrentCallDetected.set(true);
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      super.sendMessage(message);
      activeCalls.decrementAndGet();
    }

    @Override
    public void sendHeaders(Metadata headers) {
      int active = activeCalls.incrementAndGet();
      if (active > 1) {
        concurrentCallDetected.set(true);
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      super.sendHeaders(headers);
      activeCalls.decrementAndGet();
    }
  }

  private String dataPlaneServerName;
  private io.grpc.Channel dataPlaneChannel;

  private interface DataPlaneServiceHandler {
    default void sayHello(InputStream request, StreamObserver<InputStream> responseObserver) {
      responseObserver.onNext(request);
      responseObserver.onCompleted();
    }

    default StreamObserver<InputStream> sayHelloBidi(StreamObserver<InputStream> responseObserver) {
      return new StreamObserver<InputStream>() {
        @Override public void onNext(InputStream value) { responseObserver.onNext(value); }
        @Override public void onError(Throwable t) { responseObserver.onError(t); }
        @Override public void onCompleted() { responseObserver.onCompleted(); }
      };
    }
  }

  private volatile DataPlaneServiceHandler dataPlaneHandler = new DataPlaneServiceHandler() {};

  private void startDataPlane(ServerInterceptor... interceptors) throws Exception {
    dataPlaneServerName = InProcessServerBuilder.generateName();
    ServerServiceDefinition dataPlaneService = ServerServiceDefinition.builder("test.TestService")
        .addMethod(METHOD_SAY_HELLO_RAW, io.grpc.stub.ServerCalls.asyncUnaryCall(
            (request, responseObserver) -> dataPlaneHandler.sayHello(request, responseObserver)))
        .addMethod(METHOD_SAY_HELLO_BIDI, io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
            responseObserver -> dataPlaneHandler.sayHelloBidi(responseObserver)))
        .build();

    grpcCleanup.register(
        InProcessServerBuilder.forName(dataPlaneServerName)
            .addService(ServerInterceptors.intercept(dataPlaneService, java.util.Arrays.asList(interceptors)))
            .directExecutor()
            .build()
            .start());

    dataPlaneChannel = grpcCleanup.register(
        InProcessChannelBuilder.forName(dataPlaneServerName)
            .directExecutor()
            .build());
  }

  private static class SimpleServerCall extends ServerCall<InputStream, InputStream> {
    private final MethodDescriptor<InputStream, InputStream> method;

    SimpleServerCall(MethodDescriptor<InputStream, InputStream> method) {
      this.method = method;
    }

    @Override public void request(int numMessages) {}
    @Override public void sendHeaders(Metadata headers) {}
    @Override public void sendMessage(InputStream message) {}
    @Override public void close(Status status, Metadata trailers) {}
    @Override public boolean isCancelled() { return false; }
    @Override public MethodDescriptor<InputStream, InputStream> getMethodDescriptor() {
      return method;
    }
  }


  private static class InProcessNameResolverProvider extends NameResolverProvider {
    @Override
    public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
      if ("in-process".equals(targetUri.getScheme())) {
        return new NameResolver() {
          @Override public String getServiceAuthority() { return "localhost"; }
          @Override public void start(Listener2 listener) {}
          @Override public void shutdown() {}
        };
      }
      return null;
    }

    @Override protected boolean isAvailable() { return true; }
    @Override protected int priority() { return 5; }
    @Override public String getDefaultScheme() { return "in-process"; }
    @Override public Collection<Class<? extends SocketAddress>> getProducedSocketAddressTypes() {
      return Collections.emptyList();
    }
  }

  @Before
  public void setUp() throws Exception {
    NameResolverRegistry.getDefaultRegistry().register(new InProcessNameResolverProvider());

    extProcServerName = InProcessServerBuilder.generateName();
    provider = new ExternalProcessorFilter.Provider();

    bootstrapInfo =
        Bootstrapper.BootstrapInfo.builder()
            .node(Node.newBuilder().build())
            .servers(
                Collections.singletonList(
                    Bootstrapper.ServerInfo.create(
                        "test_target", Collections.emptyMap())))
            .build();

    serverInfo =
        Bootstrapper.ServerInfo.create(
            "test_target", Collections.emptyMap(), false, true, false, false);

    filterContext = Filter.FilterConfigParseContext.builder()
        .bootstrapInfo(bootstrapInfo)
        .serverInfo(serverInfo)
        .build();
  }

  private ExternalProcessor.Builder createBaseProto(String targetName) {
    return ExternalProcessor.newBuilder()
        .setGrpcService(GrpcService.newBuilder()
            .setGoogleGrpc(GrpcService.GoogleGrpc.newBuilder()
                .setTargetUri("in-process:///" + targetName)
                .addChannelCredentialsPlugin(Any.newBuilder()
                    .setTypeUrl("type.googleapis.com/envoy.extensions.grpc_service." 
                        + "channel_credentials.insecure.v3.InsecureCredentials")
                    .build())
                .build())
            .build());
  }

  // ============================================================================
  // Category 1: Configuration Parsing & Provider
  // ============================================================================

  @Test
  public void provider_registeredInFilterRegistry_basedOnFlag() {
    System.setProperty("GRPC_EXPERIMENTAL_XDS_EXT_PROC_ON_CLIENT", "true");
    try {
      FilterRegistry registry = FilterRegistry.newRegistry().register(
          new FaultFilter.Provider(),
          new RouterFilter.Provider(),
          new RbacFilter.Provider(),
          new GcpAuthenticationFilter.Provider());
      if (GrpcUtil.getFlag("GRPC_EXPERIMENTAL_XDS_EXT_PROC_ON_CLIENT", false)) {
        registry.register(new ExternalProcessorFilter.Provider());
      }
      assertThat(registry.get(ExternalProcessorFilter.TYPE_URL)).isNotNull();
    } finally {
      System.clearProperty("GRPC_EXPERIMENTAL_XDS_EXT_PROC_ON_CLIENT");
    }

    System.setProperty("GRPC_EXPERIMENTAL_XDS_EXT_PROC_ON_CLIENT", "false");
    try {
      FilterRegistry registry = FilterRegistry.newRegistry().register(
          new FaultFilter.Provider(),
          new RouterFilter.Provider(),
          new RbacFilter.Provider(),
          new GcpAuthenticationFilter.Provider());
      if (GrpcUtil.getFlag("GRPC_EXPERIMENTAL_XDS_EXT_PROC_ON_CLIENT", false)) {
        registry.register(new ExternalProcessorFilter.Provider());
      }
      assertThat(registry.get(ExternalProcessorFilter.TYPE_URL)).isNull();
    } finally {
      System.clearProperty("GRPC_EXPERIMENTAL_XDS_EXT_PROC_ON_CLIENT");
    }
  }


  // ============================================================================
  // Category 2: Configuration Override
  // ============================================================================

  @Test
  public void givenOverrideConfig_whenGrpcServiceOverridden_thenUsesNewService() throws Exception {
    ExternalProcessor parentProto = createBaseProto(extProcServerName)
        .setGrpcService(GrpcService.newBuilder()
            .setGoogleGrpc(GrpcService.GoogleGrpc.newBuilder()
                .setTargetUri("in-process:///parent")
                .addChannelCredentialsPlugin(Any.newBuilder()
                    .setTypeUrl("type.googleapis.com/envoy.extensions.grpc_service." 
                        + "channel_credentials.insecure.v3.InsecureCredentials")
                    .build())
                .build())
            .build())
        .build();
    
    GrpcService overrideService = GrpcService.newBuilder()
        .setGoogleGrpc(GrpcService.GoogleGrpc.newBuilder()
            .setTargetUri("in-process:///override")
            .addChannelCredentialsPlugin(Any.newBuilder()
                .setTypeUrl("type.googleapis.com/envoy.extensions.grpc_service." 
                    + "channel_credentials.insecure.v3.InsecureCredentials")
                .build())
            .build())
        .build();
    io.envoyproxy.envoy.extensions.filters.http.ext_proc.v3.ExtProcPerRoute perRoute =
        io.envoyproxy.envoy.extensions.filters.http.ext_proc.v3.ExtProcPerRoute.newBuilder()
            .setOverrides(io.envoyproxy.envoy.extensions.filters.http.ext_proc.v3.ExtProcOverrides.newBuilder()
                .setGrpcService(overrideService)
                .build())
            .build();

    ConfigOrError<ExternalProcessorFilterConfig> parentResult = 
        provider.parseFilterConfig(Any.pack(parentProto), filterContext);
    assertThat(parentResult.errorDetail).isNull();
    ExternalProcessorFilterConfig parentConfig = parentResult.config;
    ConfigOrError<ExternalProcessorFilterOverrideConfig> overrideResult = 
        provider.parseFilterConfigOverride(Any.pack(perRoute), filterContext);
    assertThat(overrideResult.errorDetail).isNull();
    ExternalProcessorFilterOverrideConfig overrideConfig = overrideResult.config;

    ExternalProcessorFilter filter = new ExternalProcessorFilter(FAKE_CONTEXT);
    ExternalProcessorServerInterceptor interceptor = (ExternalProcessorServerInterceptor)
        filter.buildServerInterceptor(parentConfig, overrideConfig);

    assertThat(interceptor.getFilterConfig().getExternalProcessor().getGrpcService()
        .getGoogleGrpc().getTargetUri()).isEqualTo("in-process:///override");
  }


  // ============================================================================
  // Category 3: Server Interceptor & Lifecycle
  // ============================================================================

  @Test
  @SuppressWarnings("unchecked")
  public void givenInterceptor_whenCallIntercepted_thenExtProcStubUsesDirectExecutor()
      throws Exception {
    String uniqueExtProcServerName = InProcessServerBuilder.generateName();
    ExternalProcessor proto = ExternalProcessor.newBuilder()
        .setGrpcService(GrpcService.newBuilder()
            .setGoogleGrpc(GrpcService.GoogleGrpc.newBuilder()
                .setTargetUri("in-process:///" + uniqueExtProcServerName)
                .addChannelCredentialsPlugin(Any.newBuilder()
                    .setTypeUrl("type.googleapis.com/envoy.extensions.grpc_service." 
                        + "channel_credentials.insecure.v3.InsecureCredentials")
                    .build())
                .build())
            .build())
        .build();
    ConfigOrError<ExternalProcessorFilterConfig> configOrError =
        provider.parseFilterConfig(Any.pack(proto), filterContext);
    assertThat(configOrError.errorDetail).isNull();
    ExternalProcessorFilterConfig filterConfig = configOrError.config;

    // External Processor Server
    final AtomicReference<StreamObserver<ProcessingResponse>> responseObserverRef = new AtomicReference<>();
    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          @SuppressWarnings("unchecked")
          public StreamObserver<ProcessingRequest> process(
              StreamObserver<ProcessingResponse> responseObserver) {
            responseObserverRef.set(responseObserver);
            ((ServerCallStreamObserver<ProcessingResponse>) responseObserver).request(100);
            return new StreamObserver<ProcessingRequest>() {
              @Override
              public void onNext(ProcessingRequest request) {
              }

              @Override
              public void onError(Throwable t) {
              }

              @Override
              public void onCompleted() {
                responseObserver.onCompleted();
              }
            };
          }
        };
    grpcCleanup.register(InProcessServerBuilder.forName(uniqueExtProcServerName)
        .addService(extProcImpl)
        .directExecutor()
        .build().start());

    final AtomicReference<java.util.concurrent.Executor> capturedExecutor = new AtomicReference<>();
    CachedChannelManager channelManager = new CachedChannelManager(config -> {
      return grpcCleanup.register(
          InProcessChannelBuilder.forName(uniqueExtProcServerName)
              .directExecutor()
              .intercept(new io.grpc.ClientInterceptor() {
                @Override
                public <ReqT, RespT> io.grpc.ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method, io.grpc.CallOptions callOptions, io.grpc.Channel next) {
                  if (method.equals(ExternalProcessorGrpc.getProcessMethod())) {
                    capturedExecutor.set(callOptions.getExecutor());
                  }
                  return next.newCall(method, callOptions);
                }
              })
              .build());
    });

    ExternalProcessorServerInterceptor interceptor = new ExternalProcessorServerInterceptor(
        filterConfig, channelManager, FAKE_CONTEXT);

    MutableHandlerRegistry uniqueRegistry = new MutableHandlerRegistry();
    String uniqueDataPlaneServerName = InProcessServerBuilder.generateName();
    grpcCleanup.register(InProcessServerBuilder.forName(uniqueDataPlaneServerName)
        .fallbackHandlerRegistry(uniqueRegistry)
        .directExecutor()
        .build().start());

    io.grpc.ManagedChannel dataPlaneChannel =
        grpcCleanup.register(
            InProcessChannelBuilder.forName(uniqueDataPlaneServerName).directExecutor().build());

    dataPlaneChannel.newCall(METHOD_SAY_HELLO_RAW, io.grpc.CallOptions.DEFAULT);
    
    ServerCallHandler<InputStream, InputStream> nextHandler = (call, headers) -> {
      return new ServerCall.Listener<InputStream>() {};
    };

    try {
      interceptor.interceptCall(
          new SimpleServerCall(METHOD_SAY_HELLO_RAW), new Metadata(), nextHandler);

      assertThat(capturedExecutor.get()).isNotNull();
      assertThat(capturedExecutor.get().getClass().getName()).contains("SerializingExecutor");
    } finally {
      if (responseObserverRef.get() != null) {
        responseObserverRef.get().onCompleted();
      }
      channelManager.close();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void givenGrpcServiceWithTimeout_whenCallIntercepted_thenExtProcStubHasCorrectDeadline()
      throws Exception {
    String uniqueExtProcServerName = InProcessServerBuilder.generateName();
    ExternalProcessor proto = ExternalProcessor.newBuilder()
        .setGrpcService(GrpcService.newBuilder()
            .setGoogleGrpc(GrpcService.GoogleGrpc.newBuilder()
                .setTargetUri("in-process:///" + uniqueExtProcServerName)
                .addChannelCredentialsPlugin(Any.newBuilder()
                    .setTypeUrl("type.googleapis.com/envoy.extensions.grpc_service." 
                        + "channel_credentials.insecure.v3.InsecureCredentials")
                    .build())
                .build())
            .setTimeout(com.google.protobuf.Duration.newBuilder().setSeconds(5).build())
            .build())
        .build();
    ConfigOrError<ExternalProcessorFilterConfig> configOrError =
        provider.parseFilterConfig(Any.pack(proto), filterContext);
    assertThat(configOrError.errorDetail).isNull();
    ExternalProcessorFilterConfig filterConfig = configOrError.config;

    // External Processor Server
    final AtomicReference<StreamObserver<ProcessingResponse>> responseObserverRef = new AtomicReference<>();
    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          @SuppressWarnings("unchecked")
          public StreamObserver<ProcessingRequest> process(
              StreamObserver<ProcessingResponse> responseObserver) {
            responseObserverRef.set(responseObserver);
            ((ServerCallStreamObserver<ProcessingResponse>) responseObserver).request(100);
            return new StreamObserver<ProcessingRequest>() {
              @Override
              public void onNext(ProcessingRequest request) {
              }

              @Override
              public void onError(Throwable t) {
              }

              @Override
              public void onCompleted() {
                responseObserver.onCompleted();
              }
            };
          }
        };
    grpcCleanup.register(InProcessServerBuilder.forName(uniqueExtProcServerName)
        .addService(extProcImpl)
        .directExecutor()
        .build().start());

    final AtomicReference<io.grpc.Deadline> capturedDeadline = new AtomicReference<>();
    CachedChannelManager channelManager = new CachedChannelManager(config -> {
      return grpcCleanup.register(
          InProcessChannelBuilder.forName(uniqueExtProcServerName)
              .directExecutor()
              .intercept(new io.grpc.ClientInterceptor() {
                @Override
                public <ReqT, RespT> io.grpc.ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method, io.grpc.CallOptions callOptions, io.grpc.Channel next) {
                  if (method.equals(ExternalProcessorGrpc.getProcessMethod())) {
                    capturedDeadline.set(callOptions.getDeadline());
                  }
                  return next.newCall(method, callOptions);
                }
              })
              .build());
    });

    ExternalProcessorServerInterceptor interceptor = new ExternalProcessorServerInterceptor(
        filterConfig, channelManager, FAKE_CONTEXT);

    MutableHandlerRegistry uniqueRegistry = new MutableHandlerRegistry();
    String uniqueDataPlaneServerName = InProcessServerBuilder.generateName();
    grpcCleanup.register(InProcessServerBuilder.forName(uniqueDataPlaneServerName)
        .fallbackHandlerRegistry(uniqueRegistry)
        .directExecutor()
        .build().start());

    io.grpc.ManagedChannel dataPlaneChannel =
        grpcCleanup.register(
            InProcessChannelBuilder.forName(uniqueDataPlaneServerName).directExecutor().build());

    dataPlaneChannel.newCall(METHOD_SAY_HELLO_RAW, io.grpc.CallOptions.DEFAULT);
    
    ServerCallHandler<InputStream, InputStream> nextHandler = (call, headers) -> {
      return new ServerCall.Listener<InputStream>() {};
    };

    try {
      interceptor.interceptCall(
          new SimpleServerCall(METHOD_SAY_HELLO_RAW), new Metadata(), nextHandler);

      assertThat(capturedDeadline.get()).isNotNull();
      assertThat(capturedDeadline.get().timeRemaining(TimeUnit.SECONDS)).isAtLeast(4);
    } finally {
      if (responseObserverRef.get() != null) {
        responseObserverRef.get().onCompleted();
      }
      channelManager.close();
    }
  }




  // ============================================================================
  // Category 4: Request Header Processing
  // ============================================================================

  @Test
  public void serverInterceptor_headerMutation_addsHeader() throws Exception {
    ExternalProcessor proto = createBaseProto(extProcServerName).build();
    ConfigOrError<ExternalProcessorFilterConfig> configOrError =
        provider.parseFilterConfig(Any.pack(proto), filterContext);
    assertThat(configOrError.errorDetail).isNull();
    ExternalProcessorFilterConfig filterConfig = configOrError.config;

    final CountDownLatch extProcLatch = new CountDownLatch(1);
    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          @SuppressWarnings("unchecked")
          public StreamObserver<ProcessingRequest> process(
              StreamObserver<ProcessingResponse> responseObserver) {
            ((ServerCallStreamObserver<ProcessingResponse>) responseObserver).request(100);
            return new StreamObserver<ProcessingRequest>() {
              @Override
              public void onNext(ProcessingRequest request) {
                if (request.hasRequestHeaders()) {
                  responseObserver.onNext(ProcessingResponse.newBuilder()
                      .setRequestHeaders(HeadersResponse.newBuilder()
                          .setResponse(CommonResponse.newBuilder()
                              .setHeaderMutation(HeaderMutation.newBuilder()
                                  .addSetHeaders(io.envoyproxy.envoy.config.core.v3.HeaderValueOption.newBuilder()
                                      .setHeader(io.envoyproxy.envoy.config.core.v3.HeaderValue.newBuilder()
                                          .setKey("x-mutated-header")
                                          .setValue("mutated-value")
                                          .build())
                                      .build())
                                  .build())
                              .build())
                          .build())
                      .build());
                  responseObserver.onCompleted();
                  extProcLatch.countDown();
                }
              }

              @Override
              public void onError(Throwable t) {
              }

              @Override
              public void onCompleted() {
                responseObserver.onCompleted();
              }
            };
          }
        };

    grpcCleanup.register(InProcessServerBuilder.forName(extProcServerName)
        .addService(extProcImpl)
        .directExecutor()
        .build().start());

    CachedChannelManager channelManager = new CachedChannelManager(config -> {
      return grpcCleanup.register(
          InProcessChannelBuilder.forName(extProcServerName)
              .directExecutor()
              .build());
    });

    ExternalProcessorServerInterceptor interceptor = new ExternalProcessorServerInterceptor(
        filterConfig, channelManager, FAKE_CONTEXT);

    final AtomicReference<Metadata> receivedHeaders = new AtomicReference<>();
    ServerInterceptor capturingInterceptor = new ServerInterceptor() {
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
          ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        receivedHeaders.set(headers);
        return next.startCall(call, headers);
      }
    };

    startDataPlane(interceptor, capturingInterceptor);

    Metadata initialHeaders = new Metadata();
    initialHeaders.put(Metadata.Key.of("x-initial-header", Metadata.ASCII_STRING_MARSHALLER), "initial-value");

    io.grpc.ClientCall<InputStream, InputStream> clientCall = dataPlaneChannel.newCall(
        METHOD_SAY_HELLO_RAW, io.grpc.CallOptions.DEFAULT);

    final CountDownLatch callCompletedLatch = new CountDownLatch(1);
    clientCall.start(new io.grpc.ClientCall.Listener<InputStream>() {
      @Override
      public void onClose(Status status, Metadata trailers) {
        callCompletedLatch.countDown();
      }
    }, initialHeaders);

    clientCall.request(1);
    clientCall.sendMessage(new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)));
    clientCall.halfClose();

    assertThat(extProcLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(callCompletedLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(receivedHeaders.get()).isNotNull();
    assertThat(receivedHeaders.get().get(Metadata.Key.of("x-mutated-header", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("mutated-value");
  }

  @Test
  public void serverInterceptor_requestHeaderModeSkip_doesNotSendHeaders() throws Exception {
    ExternalProcessor proto = createBaseProto(extProcServerName)
        .setProcessingMode(ProcessingMode.newBuilder()
            .setRequestHeaderMode(ProcessingMode.HeaderSendMode.SKIP)
            .setResponseHeaderMode(ProcessingMode.HeaderSendMode.SKIP)
            .setResponseTrailerMode(ProcessingMode.HeaderSendMode.SKIP)
            .build())
        .build();
    ConfigOrError<ExternalProcessorFilterConfig> configOrError =
        provider.parseFilterConfig(Any.pack(proto), filterContext);
    assertThat(configOrError.errorDetail).isNull();
    ExternalProcessorFilterConfig filterConfig = configOrError.config;

    final CountDownLatch extProcLatch = new CountDownLatch(1);
    final AtomicBoolean requestHeadersReceived = new AtomicBoolean(false);
    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          public StreamObserver<ProcessingRequest> process(
              StreamObserver<ProcessingResponse> responseObserver) {
            return new StreamObserver<ProcessingRequest>() {
              @Override
              public void onNext(ProcessingRequest request) {
                if (request.hasRequestHeaders()) {
                  requestHeadersReceived.set(true);
                } else if (request.hasResponseBody()) {
                  responseObserver.onCompleted();
                  extProcLatch.countDown();
                }
              }

              @Override public void onError(Throwable t) {}
              @Override public void onCompleted() {}
            };
          }
        };

    grpcCleanup.register(InProcessServerBuilder.forName(extProcServerName)
        .addService(extProcImpl)
        .directExecutor()
        .build().start());

    CachedChannelManager channelManager = new CachedChannelManager(config -> {
      return grpcCleanup.register(
          InProcessChannelBuilder.forName(extProcServerName)
              .directExecutor()
              .build());
    });

    ExternalProcessorServerInterceptor interceptor = new ExternalProcessorServerInterceptor(
        filterConfig, channelManager, FAKE_CONTEXT);

    final AtomicReference<Metadata> receivedHeaders = new AtomicReference<>();
    ServerInterceptor capturingInterceptor = new ServerInterceptor() {
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
          ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        receivedHeaders.set(headers);
        return next.startCall(call, headers);
      }
    };

    startDataPlane(interceptor, capturingInterceptor);

    Metadata initialHeaders = new Metadata();
    initialHeaders.put(Metadata.Key.of("x-initial-header", Metadata.ASCII_STRING_MARSHALLER), "initial-value");

    io.grpc.ClientCall<InputStream, InputStream> clientCall = dataPlaneChannel.newCall(
        METHOD_SAY_HELLO_RAW, io.grpc.CallOptions.DEFAULT);

    final CountDownLatch callCompletedLatch = new CountDownLatch(1);
    clientCall.start(new io.grpc.ClientCall.Listener<InputStream>() {
      @Override
      public void onClose(Status status, Metadata trailers) {
        callCompletedLatch.countDown();
      }
    }, initialHeaders);

    clientCall.request(1);
    clientCall.sendMessage(new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)));
    clientCall.halfClose();

    assertThat(extProcLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(callCompletedLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(requestHeadersReceived.get()).isFalse();
    assertThat(receivedHeaders.get()).isNotNull();
    assertThat(receivedHeaders.get().get(Metadata.Key.of("x-initial-header", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("initial-value");
  }

  @Test
  public void givenRequestHeaderModeSend_whenStartCallCalled_thenCallIsBuffered() throws Exception {
    // Configure GRPC request body mode
    ExternalProcessor proto = createBaseProto(extProcServerName)
        .setProcessingMode(ProcessingMode.newBuilder()
            .setRequestBodyMode(ProcessingMode.BodySendMode.GRPC)
            .build())
        .build();
    ConfigOrError<ExternalProcessorFilterConfig> configOrError =
        provider.parseFilterConfig(Any.pack(proto), filterContext);
    assertThat(configOrError.errorDetail).isNull();
    ExternalProcessorFilterConfig filterConfig = configOrError.config;

    final AtomicReference<ProcessingRequest> receivedRequestRef = new AtomicReference<>();
    final CountDownLatch requestLatch = new CountDownLatch(2); // Expect 1 for headers, 1 for body
    final AtomicReference<StreamObserver<ProcessingResponse>> responseObserverRef = new AtomicReference<>();

    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          public StreamObserver<ProcessingRequest> process(
              StreamObserver<ProcessingResponse> responseObserver) {
            ((ServerCallStreamObserver<ProcessingResponse>) responseObserver).request(100);
            responseObserverRef.set(responseObserver);
            return new StreamObserver<ProcessingRequest>() {
              @Override
              public void onNext(ProcessingRequest request) {
                if (request.hasRequestBody()) {
                  receivedRequestRef.set(request);
                }
                requestLatch.countDown();
              }
              @Override public void onError(Throwable t) {}
              @Override public void onCompleted() {}
            };
          }
        };

    grpcCleanup.register(InProcessServerBuilder.forName(extProcServerName)
        .addService(extProcImpl)
        .directExecutor()
        .build().start());

    CachedChannelManager channelManager = new CachedChannelManager(config -> {
      return grpcCleanup.register(
          InProcessChannelBuilder.forName(extProcServerName)
              .directExecutor()
              .build());
    });

    ExternalProcessorServerInterceptor interceptor = new ExternalProcessorServerInterceptor(
        filterConfig, channelManager, FAKE_CONTEXT);

    ServerCall<InputStream, InputStream> dummyCall = new ServerCall<InputStream, InputStream>() {
      @Override public void request(int numMessages) {}
      @Override public void sendHeaders(Metadata headers) {}
      @Override public void sendMessage(InputStream message) {}
      @Override public void close(Status status, Metadata trailers) {}
      @Override public boolean isCancelled() { return false; }
      @Override public MethodDescriptor<InputStream, InputStream> getMethodDescriptor() {
        return METHOD_SAY_HELLO_RAW;
      }
    };

    final AtomicBoolean startCallCalled = new AtomicBoolean();
    ServerCallHandler<InputStream, InputStream> dummyNext = new ServerCallHandler<InputStream, InputStream>() {
      @Override
      public ServerCall.Listener<InputStream> startCall(ServerCall<InputStream, InputStream> call, Metadata headers) {
        startCallCalled.set(true);
        return new ServerCall.Listener<InputStream>() {};
      }
    };

    @SuppressWarnings("unchecked")
    ServerCall.Listener<InputStream> serverListener =
        (ServerCall.Listener<InputStream>) (ServerCall.Listener<?>)
            interceptor.interceptCall(dummyCall, new Metadata(), dummyNext);

    // Invocate onMessage() while the call is IDLE (headers response has not been sent)
    byte[] messageBytes = "hello".getBytes(StandardCharsets.UTF_8);
    serverListener.onMessage(new ByteArrayInputStream(messageBytes));

    // Wait and verify that the external processor immediately receives both the headers request and the body request
    boolean receivedInTime = requestLatch.await(5, TimeUnit.SECONDS);
    assertThat(receivedInTime).isTrue();

    ProcessingRequest receivedRequest = receivedRequestRef.get();
    assertThat(receivedRequest).isNotNull();
    assertThat(receivedRequest.hasRequestBody()).isTrue();
    assertThat(receivedRequest.getRequestBody().getBody().toStringUtf8()).isEqualTo("hello");

    // Assert that the data plane call was not started yet (buffered)
    assertThat(startCallCalled.get()).isFalse();

    // Clean up control stream to allow resources to be released cleanly
    StreamObserver<ProcessingResponse> responseObserver = responseObserverRef.get();
    if (responseObserver != null) {
      responseObserver.onCompleted();
    }
  }

  @Test
  public void givenRequestHeaderModeSend_whenExtProcRespondsWithMutations_thenCallIsActivated() throws Exception {
    // Configure GRPC request body mode
    ExternalProcessor proto = createBaseProto(extProcServerName)
        .setProcessingMode(ProcessingMode.newBuilder()
            .setRequestBodyMode(ProcessingMode.BodySendMode.GRPC)
            .build())
        .build();
    ConfigOrError<ExternalProcessorFilterConfig> configOrError =
        provider.parseFilterConfig(Any.pack(proto), filterContext);
    assertThat(configOrError.errorDetail).isNull();
    ExternalProcessorFilterConfig filterConfig = configOrError.config;

    final AtomicReference<StreamObserver<ProcessingResponse>> responseObserverRef = new AtomicReference<>();
    final CountDownLatch requestLatch = new CountDownLatch(1); // Just wait for headers request to be processed

    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          public StreamObserver<ProcessingRequest> process(
              StreamObserver<ProcessingResponse> responseObserver) {
            ((ServerCallStreamObserver<ProcessingResponse>) responseObserver).request(100);
            responseObserverRef.set(responseObserver);
            return new StreamObserver<ProcessingRequest>() {
              @Override
              public void onNext(ProcessingRequest request) {
                requestLatch.countDown();
              }
              @Override public void onError(Throwable t) {}
              @Override public void onCompleted() {}
            };
          }
        };

    grpcCleanup.register(InProcessServerBuilder.forName(extProcServerName)
        .addService(extProcImpl)
        .directExecutor()
        .build().start());

    CachedChannelManager channelManager = new CachedChannelManager(config -> {
      return grpcCleanup.register(
          InProcessChannelBuilder.forName(extProcServerName)
              .directExecutor()
              .build());
    });

    ExternalProcessorServerInterceptor interceptor = new ExternalProcessorServerInterceptor(
        filterConfig, channelManager, FAKE_CONTEXT);

    ServerCall<InputStream, InputStream> dummyCall = new ServerCall<InputStream, InputStream>() {
      @Override public void request(int numMessages) {}
      @Override public void sendHeaders(Metadata headers) {}
      @Override public void sendMessage(InputStream message) {}
      @Override public void close(Status status, Metadata trailers) {}
      @Override public boolean isCancelled() { return false; }
      @Override public MethodDescriptor<InputStream, InputStream> getMethodDescriptor() {
        return METHOD_SAY_HELLO_RAW;
      }
    };

    final AtomicBoolean startCallCalled = new AtomicBoolean();
    ServerCallHandler<InputStream, InputStream> dummyNext = new ServerCallHandler<InputStream, InputStream>() {
      @Override
      public ServerCall.Listener<InputStream> startCall(ServerCall<InputStream, InputStream> call, Metadata headers) {
        startCallCalled.set(true);
        return new ServerCall.Listener<InputStream>() {};
      }
    };

    @SuppressWarnings("unchecked")
    ServerCall.Listener<InputStream> serverListener =
        (ServerCall.Listener<InputStream>) (ServerCall.Listener<?>)
            interceptor.interceptCall(dummyCall, new Metadata(), dummyNext);

    // Invocate onMessage() while the call is IDLE (headers response has not been sent)
    byte[] messageBytes = "hello".getBytes(StandardCharsets.UTF_8);
    serverListener.onMessage(new ByteArrayInputStream(messageBytes));

    boolean receivedInTime = requestLatch.await(5, TimeUnit.SECONDS);
    assertThat(receivedInTime).isTrue();

    // Assert that the data plane call was not started yet
    assertThat(startCallCalled.get()).isFalse();

    // Clean up control stream by completing it, which triggers fallback activation on the data plane call
    StreamObserver<ProcessingResponse> responseObserver = responseObserverRef.get();
    assertThat(responseObserver).isNotNull();
    responseObserver.onCompleted();

    // Verify that the call is now activated
    assertThat(startCallCalled.get()).isTrue();
  }

  // ============================================================================
  // Category 5: Body Mutation: Outbound/Request (GRPC Mode)
  // ============================================================================

  @Test
  public void serverInterceptor_responseHeaderMutation_mutatesHeader() throws Exception {
    ExternalProcessor proto = createBaseProto(extProcServerName).build();
    ConfigOrError<ExternalProcessorFilterConfig> configOrError =
        provider.parseFilterConfig(Any.pack(proto), filterContext);
    assertThat(configOrError.errorDetail).isNull();
    ExternalProcessorFilterConfig filterConfig = configOrError.config;

    final CountDownLatch extProcLatch = new CountDownLatch(2); // 1 for request headers, 1 for response headers
    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          @SuppressWarnings("unchecked")
          public StreamObserver<ProcessingRequest> process(
              StreamObserver<ProcessingResponse> responseObserver) {
            ((ServerCallStreamObserver<ProcessingResponse>) responseObserver).request(100);
            return new StreamObserver<ProcessingRequest>() {
              @Override
              public void onNext(ProcessingRequest request) {
                if (request.hasRequestHeaders()) {
                  responseObserver.onNext(ProcessingResponse.newBuilder()
                      .setRequestHeaders(HeadersResponse.newBuilder()
                          .setResponse(CommonResponse.newBuilder().build())
                          .build())
                      .build());
                  extProcLatch.countDown();
                } else if (request.hasResponseHeaders()) {
                  responseObserver.onNext(ProcessingResponse.newBuilder()
                      .setResponseHeaders(HeadersResponse.newBuilder()
                          .setResponse(CommonResponse.newBuilder()
                              .setHeaderMutation(HeaderMutation.newBuilder()
                                  .addSetHeaders(io.envoyproxy.envoy.config.core.v3.HeaderValueOption.newBuilder()
                                      .setHeader(io.envoyproxy.envoy.config.core.v3.HeaderValue.newBuilder()
                                          .setKey("x-mutated-response-header")
                                          .setValue("mutated-response-value")
                                          .build())
                                      .build())
                                  .build())
                              .build())
                          .build())
                      .build());
                  responseObserver.onCompleted();
                  extProcLatch.countDown();
                }
              }

              @Override public void onError(Throwable t) {}
              @Override public void onCompleted() {}
            };
          }
        };

    grpcCleanup.register(InProcessServerBuilder.forName(extProcServerName)
        .addService(extProcImpl)
        .directExecutor()
        .build().start());

    CachedChannelManager channelManager = new CachedChannelManager(config -> {
      return grpcCleanup.register(
          InProcessChannelBuilder.forName(extProcServerName)
              .directExecutor()
              .build());
    });

    ExternalProcessorServerInterceptor interceptor = new ExternalProcessorServerInterceptor(
        filterConfig, channelManager, FAKE_CONTEXT);

    dataPlaneHandler = new DataPlaneServiceHandler() {
      @Override
      public void sayHello(InputStream request, StreamObserver<InputStream> responseObserver) {
        responseObserver.onNext(request);
        responseObserver.onCompleted();
      }
    };

    ServerInterceptor headersInterceptor = new ServerInterceptor() {
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
          ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
          @Override
          public void sendHeaders(Metadata responseHeaders) {
            responseHeaders.put(Metadata.Key.of("x-initial-response-header", Metadata.ASCII_STRING_MARSHALLER), "initial-response-value");
            super.sendHeaders(responseHeaders);
          }
        }, headers);
      }
    };

    startDataPlane(interceptor, headersInterceptor);

    io.grpc.ClientCall<InputStream, InputStream> clientCall = dataPlaneChannel.newCall(
        METHOD_SAY_HELLO_RAW, io.grpc.CallOptions.DEFAULT);

    final AtomicReference<Metadata> receivedResponseHeaders = new AtomicReference<>();
    final CountDownLatch callCompletedLatch = new CountDownLatch(1);
    clientCall.start(new io.grpc.ClientCall.Listener<InputStream>() {
      @Override
      public void onHeaders(Metadata headers) {
        receivedResponseHeaders.set(headers);
      }
      @Override
      public void onClose(Status status, Metadata trailers) {
        callCompletedLatch.countDown();
      }
    }, new Metadata());

    clientCall.request(1);
    clientCall.sendMessage(new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)));
    clientCall.halfClose();

    assertThat(extProcLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(callCompletedLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(receivedResponseHeaders.get()).isNotNull();
    assertThat(receivedResponseHeaders.get().get(Metadata.Key.of("x-mutated-response-header", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("mutated-response-value");
    assertThat(receivedResponseHeaders.get().get(Metadata.Key.of("x-initial-response-header", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("initial-response-value");
  }

  // ============================================================================
  // Category 6: Body Mutation: Inbound/Response (GRPC Mode)
  // ============================================================================

  @Test
  public void serverInterceptor_responseHeaderModeSkip_doesNotSendResponseHeaders() throws Exception {
    ExternalProcessor proto = createBaseProto(extProcServerName)
        .setProcessingMode(ProcessingMode.newBuilder()
            .setResponseHeaderMode(ProcessingMode.HeaderSendMode.SKIP)
            .build())
        .build();
    ConfigOrError<ExternalProcessorFilterConfig> configOrError =
        provider.parseFilterConfig(Any.pack(proto), filterContext);
    assertThat(configOrError.errorDetail).isNull();
    ExternalProcessorFilterConfig filterConfig = configOrError.config;

    final CountDownLatch extProcLatch = new CountDownLatch(1); // 1 for request headers
    final AtomicBoolean responseHeadersReceived = new AtomicBoolean(false);
    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          @SuppressWarnings("unchecked")
          public StreamObserver<ProcessingRequest> process(
              StreamObserver<ProcessingResponse> responseObserver) {
            ((ServerCallStreamObserver<ProcessingResponse>) responseObserver).request(100);
            return new StreamObserver<ProcessingRequest>() {
              @Override
              public void onNext(ProcessingRequest request) {
                if (request.hasRequestHeaders()) {
                  responseObserver.onNext(ProcessingResponse.newBuilder()
                      .setRequestHeaders(HeadersResponse.newBuilder()
                          .setResponse(CommonResponse.newBuilder().build())
                          .build())
                      .build());
                  extProcLatch.countDown();
                } else if (request.hasResponseHeaders()) {
                  responseHeadersReceived.set(true);
                } else if (request.hasResponseBody()) {
                  responseObserver.onCompleted();
                }
              }

              @Override public void onError(Throwable t) {}
              @Override public void onCompleted() {}
            };
          }
        };

    grpcCleanup.register(InProcessServerBuilder.forName(extProcServerName)
        .addService(extProcImpl)
        .directExecutor()
        .build().start());

    CachedChannelManager channelManager = new CachedChannelManager(config -> {
      return grpcCleanup.register(
          InProcessChannelBuilder.forName(extProcServerName)
              .directExecutor()
              .build());
    });

    ExternalProcessorServerInterceptor interceptor = new ExternalProcessorServerInterceptor(
        filterConfig, channelManager, FAKE_CONTEXT);

    dataPlaneHandler = new DataPlaneServiceHandler() {
      @Override
      public void sayHello(InputStream request, StreamObserver<InputStream> responseObserver) {
        responseObserver.onNext(request);
        responseObserver.onCompleted();
      }
    };

    ServerInterceptor headersInterceptor = new ServerInterceptor() {
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
          ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
          @Override
          public void sendHeaders(Metadata responseHeaders) {
            responseHeaders.put(Metadata.Key.of("x-initial-response-header", Metadata.ASCII_STRING_MARSHALLER), "initial-response-value");
            super.sendHeaders(responseHeaders);
          }
        }, headers);
      }
    };

    startDataPlane(interceptor, headersInterceptor);

    io.grpc.ClientCall<InputStream, InputStream> clientCall = dataPlaneChannel.newCall(
        METHOD_SAY_HELLO_RAW, io.grpc.CallOptions.DEFAULT);

    final AtomicReference<Metadata> receivedResponseHeaders = new AtomicReference<>();
    final CountDownLatch callCompletedLatch = new CountDownLatch(1);
    clientCall.start(new io.grpc.ClientCall.Listener<InputStream>() {
      @Override
      public void onHeaders(Metadata headers) {
        receivedResponseHeaders.set(headers);
      }
      @Override
      public void onClose(Status status, Metadata trailers) {
        callCompletedLatch.countDown();
      }
    }, new Metadata());

    clientCall.request(1);
    clientCall.sendMessage(new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)));
    clientCall.halfClose();

    assertThat(extProcLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(callCompletedLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(responseHeadersReceived.get()).isFalse();
    assertThat(receivedResponseHeaders.get()).isNotNull();
    assertThat(receivedResponseHeaders.get().get(Metadata.Key.of("x-initial-response-header", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("initial-response-value");
  }

  // ============================================================================
  // Category 7: Outbound Backpressure (isReady / onReady)
  // ============================================================================

  @Test
  @SuppressWarnings("unchecked")
  public void givenObservabilityTrue_whenExtProcBusy_thenIsReadyReturnsFalse()
      throws Exception {
    String uniqueExtProcServerName = InProcessServerBuilder.generateName();
    ExternalProcessor proto = ExternalProcessor.newBuilder()
        .setGrpcService(GrpcService.newBuilder()
            .setGoogleGrpc(GrpcService.GoogleGrpc.newBuilder()
                .setTargetUri("in-process:///" + uniqueExtProcServerName)
                .addChannelCredentialsPlugin(Any.newBuilder()
                    .setTypeUrl("type.googleapis.com/envoy.extensions.grpc_service." 
                        + "channel_credentials.insecure.v3.InsecureCredentials")
                    .build())
                .build())
            .build())
        .setObservabilityMode(true)
        .build();
    ConfigOrError<ExternalProcessorFilterConfig> configOrError =
        provider.parseFilterConfig(Any.pack(proto), filterContext);
    assertThat(configOrError.errorDetail).isNull();
    ExternalProcessorFilterConfig filterConfig = configOrError.config;

    // External Processor Server
    final AtomicReference<StreamObserver<ProcessingResponse>> responseObserverRef = new AtomicReference<>();
    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          @SuppressWarnings("unchecked")
          public StreamObserver<ProcessingRequest> process(
              final StreamObserver<ProcessingResponse> responseObserver) {
            responseObserverRef.set(responseObserver);
            ((ServerCallStreamObserver<ProcessingResponse>) responseObserver).request(100);
            return new StreamObserver<ProcessingRequest>() {
              @Override
              public void onNext(ProcessingRequest request) {
                if (request.hasRequestHeaders()) {
                  responseObserver.onNext(ProcessingResponse.newBuilder()
                      .setRequestHeaders(HeadersResponse.newBuilder().build())
                      .build());
                }
              }

              @Override public void onError(Throwable t) {}
              @Override public void onCompleted() { responseObserver.onCompleted(); }
            };
          }
        };
    grpcCleanup.register(InProcessServerBuilder.forName(uniqueExtProcServerName)
        .addService(extProcImpl)
        .directExecutor()
        .build().start());

    final AtomicBoolean sidecarReady = new AtomicBoolean(true);
    CachedChannelManager channelManager = new CachedChannelManager(config -> {
      return grpcCleanup.register(
          InProcessChannelBuilder.forName(uniqueExtProcServerName)
              .directExecutor()
              .intercept(new io.grpc.ClientInterceptor() {
                @Override
                public <ReqT, RespT> io.grpc.ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method, io.grpc.CallOptions callOptions, io.grpc.Channel next) {
                  return new io.grpc.ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                      next.newCall(method, callOptions)) {
                    @Override
                    public boolean isReady() {
                      return sidecarReady.get();
                    }
                  };
                }
              })
              .build());
    });

    ExternalProcessorServerInterceptor interceptor = new ExternalProcessorServerInterceptor(
        filterConfig, channelManager, FAKE_CONTEXT);

    final AtomicReference<ServerCall<InputStream, InputStream>> interceptedCallRef = new AtomicReference<>();
    ServerCallHandler<InputStream, InputStream> nextHandler = (call, headers) -> {
      interceptedCallRef.set(call);
      return new ServerCall.Listener<InputStream>() {};
    };

    final AtomicBoolean rawCallReady = new AtomicBoolean(true);
    ServerCall<InputStream, InputStream> rawCall = new SimpleServerCall(METHOD_SAY_HELLO_RAW) {
      @Override
      public boolean isReady() {
        return rawCallReady.get();
      }
    };

    try {
      interceptor.interceptCall(rawCall, new Metadata(), nextHandler);

      // Wait for activation (ext_proc response received)
      long startTime = System.currentTimeMillis();
      while (!interceptedCallRef.get().isReady() && System.currentTimeMillis() - startTime < 5000) {
        Thread.sleep(10);
      }
      assertThat(interceptedCallRef.get().isReady()).isTrue();

      // Ext proc busy
      sidecarReady.set(false);
      assertThat(interceptedCallRef.get().isReady()).isFalse();

      // Ext proc ready again
      sidecarReady.set(true);
      assertThat(interceptedCallRef.get().isReady()).isTrue();
      
      // Raw call not ready
      rawCallReady.set(false);
      assertThat(interceptedCallRef.get().isReady()).isFalse();
    } finally {
      if (responseObserverRef.get() != null) {
        try {
          responseObserverRef.get().onCompleted();
        } catch (IllegalStateException ignored) {
          // Ignore if already closed
        }
      }
      channelManager.close();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void givenRequestDrainActive_whenIsReadyCalled_thenReturnsFalse() throws Exception {
    String uniqueExtProcServerName = InProcessServerBuilder.generateName();
    ExternalProcessor proto = ExternalProcessor.newBuilder()
        .setGrpcService(GrpcService.newBuilder()
            .setGoogleGrpc(GrpcService.GoogleGrpc.newBuilder()
                .setTargetUri("in-process:///" + uniqueExtProcServerName)
                .addChannelCredentialsPlugin(Any.newBuilder()
                    .setTypeUrl("type.googleapis.com/envoy.extensions.grpc_service." 
                        + "channel_credentials.insecure.v3.InsecureCredentials")
                    .build())
                .build())
            .build())
        .build();
    ConfigOrError<ExternalProcessorFilterConfig> configOrError =
        provider.parseFilterConfig(Any.pack(proto), filterContext);
    assertThat(configOrError.errorDetail).isNull();
    ExternalProcessorFilterConfig filterConfig = configOrError.config;

    final CountDownLatch drainLatch = new CountDownLatch(1);
    final AtomicReference<StreamObserver<ProcessingResponse>> responseObserverRef = new AtomicReference<>();
    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          @SuppressWarnings("unchecked")
          public StreamObserver<ProcessingRequest> process(
              final StreamObserver<ProcessingResponse> responseObserver) {
            responseObserverRef.set(responseObserver);
            ((ServerCallStreamObserver<ProcessingResponse>) responseObserver).request(100);
            return new StreamObserver<ProcessingRequest>() {
              @Override
              public void onNext(ProcessingRequest request) {
                if (request.hasRequestHeaders()) {
                  responseObserver.onNext(ProcessingResponse.newBuilder()
                      .setRequestDrain(true)
                      .build());
                  drainLatch.countDown();
                }
              }

              @Override public void onError(Throwable t) {}
              @Override public void onCompleted() {}
            };
          }
        };
    grpcCleanup.register(InProcessServerBuilder.forName(uniqueExtProcServerName)
        .addService(extProcImpl)
        .directExecutor()
        .build().start());

    CachedChannelManager channelManager = new CachedChannelManager(config -> {
      return grpcCleanup.register(
          InProcessChannelBuilder.forName(uniqueExtProcServerName).directExecutor().build());
    });

    ExternalProcessorServerInterceptor interceptor = new ExternalProcessorServerInterceptor(
        filterConfig, channelManager, FAKE_CONTEXT);

    final AtomicReference<ServerCall<InputStream, InputStream>> interceptedCallRef = new AtomicReference<>();
    ServerCallHandler<InputStream, InputStream> nextHandler = (call, headers) -> {
      interceptedCallRef.set(call);
      return new ServerCall.Listener<InputStream>() {};
    };

    try {
      interceptor.interceptCall(new SimpleServerCall(METHOD_SAY_HELLO_RAW), new Metadata(), nextHandler);

      assertThat(drainLatch.await(5, TimeUnit.SECONDS)).isTrue();

      // isReady() must return false during drain.
      long start = System.currentTimeMillis();
      while (interceptedCallRef.get().isReady() && System.currentTimeMillis() - start < 2000) {
        Thread.sleep(10);
      }
      assertThat(interceptedCallRef.get().isReady()).isFalse();
    } finally {
      if (responseObserverRef.get() != null) {
        try {
          responseObserverRef.get().onCompleted();
        } catch (IllegalStateException ignored) {
          // Ignore if already closed
        }
      }
      channelManager.close();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void givenCongestionInExtProc_whenExtProcBecomesReady_thenTriggersOnReady()
      throws Exception {
    String uniqueExtProcServerName = InProcessServerBuilder.generateName();
    ExternalProcessor proto = ExternalProcessor.newBuilder()
        .setGrpcService(GrpcService.newBuilder()
            .setGoogleGrpc(GrpcService.GoogleGrpc.newBuilder()
                .setTargetUri("in-process:///" + uniqueExtProcServerName)
                .addChannelCredentialsPlugin(Any.newBuilder()
                    .setTypeUrl("type.googleapis.com/envoy.extensions.grpc_service." 
                        + "channel_credentials.insecure.v3.InsecureCredentials")
                    .build())
                .build())
            .build())
        .setObservabilityMode(true)
        .build();
    ConfigOrError<ExternalProcessorFilterConfig> configOrError =
        provider.parseFilterConfig(Any.pack(proto), filterContext);
    assertThat(configOrError.errorDetail).isNull();
    ExternalProcessorFilterConfig filterConfig = configOrError.config;

    // External Processor Server
    final AtomicReference<StreamObserver<ProcessingResponse>> responseObserverRef = new AtomicReference<>();
    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          @SuppressWarnings("unchecked")
          public StreamObserver<ProcessingRequest> process(
              StreamObserver<ProcessingResponse> responseObserver) {
            responseObserverRef.set(responseObserver);
            ((ServerCallStreamObserver<ProcessingResponse>) responseObserver).request(100);
            return new StreamObserver<ProcessingRequest>() {
              @Override public void onNext(ProcessingRequest request) {}
              @Override public void onError(Throwable t) {}
              @Override public void onCompleted() { responseObserver.onCompleted(); }
            };
          }
        };
    grpcCleanup.register(InProcessServerBuilder.forName(uniqueExtProcServerName)
        .addService(extProcImpl)
        .directExecutor()
        .build().start());

    final AtomicReference<ClientCall.Listener<ProcessingResponse>> sidecarListenerRef =
        new AtomicReference<>();
    CachedChannelManager channelManager = new CachedChannelManager(config -> {
      return grpcCleanup.register(
          InProcessChannelBuilder.forName(uniqueExtProcServerName)
              .directExecutor()
              .intercept(new io.grpc.ClientInterceptor() {
                @Override
                public <ReqT, RespT> io.grpc.ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method, io.grpc.CallOptions callOptions, io.grpc.Channel next) {
                  return new io.grpc.ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                      next.newCall(method, callOptions)) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                      sidecarListenerRef.set((Listener<ProcessingResponse>) responseListener);
                      super.start(responseListener, headers);
                    }
                  };
                }
              })
              .build());
    });

    ExternalProcessorServerInterceptor interceptor = new ExternalProcessorServerInterceptor(
        filterConfig, channelManager, FAKE_CONTEXT);

    final CountDownLatch onReadyLatch = new CountDownLatch(1);
    ServerCall.Listener<InputStream> appListener = new ServerCall.Listener<InputStream>() {
      @Override
      public void onReady() {
        onReadyLatch.countDown();
      }
    };

    ServerCallHandler<InputStream, InputStream> nextHandler = (call, headers) -> {
      return appListener;
    };

    try {
      interceptor.interceptCall(
          new SimpleServerCall(METHOD_SAY_HELLO_RAW), new Metadata(), nextHandler);

      // Wait for sidecar call to start and listener to be captured
      long startTime = System.currentTimeMillis();
      while (sidecarListenerRef.get() == null && System.currentTimeMillis() - startTime < 5000) {
        Thread.sleep(10);
      }
      assertThat(sidecarListenerRef.get()).isNotNull();

      // Trigger sidecar onReady
      sidecarListenerRef.get().onReady();

      // Verify app listener notified
      assertThat(onReadyLatch.await(5, TimeUnit.SECONDS)).isTrue();
    } finally {
      if (responseObserverRef.get() != null) {
        try {
          responseObserverRef.get().onCompleted();
        } catch (IllegalStateException ignored) {
          // Ignore if already closed
        }
      }
      channelManager.close();
    }
  }


  // ============================================================================
  // Category 8: Inbound Backpressure (request(n) / pendingRequests)
  // ============================================================================

  @Test
  @SuppressWarnings("unchecked")
  public void givenObservabilityTrue_whenExtProcBusy_thenAppRequestsBuffered()
      throws Exception {
    String uniqueExtProcServerName = InProcessServerBuilder.generateName();
    ExternalProcessor proto = ExternalProcessor.newBuilder()
        .setGrpcService(GrpcService.newBuilder()
            .setGoogleGrpc(GrpcService.GoogleGrpc.newBuilder()
                .setTargetUri("in-process:///" + uniqueExtProcServerName)
                .addChannelCredentialsPlugin(Any.newBuilder()
                    .setTypeUrl("type.googleapis.com/envoy.extensions.grpc_service." 
                        + "channel_credentials.insecure.v3.InsecureCredentials")
                    .build())
                .build())
            .build())
        .setObservabilityMode(true)
        .build();
    ConfigOrError<ExternalProcessorFilterConfig> configOrError =
        provider.parseFilterConfig(Any.pack(proto), filterContext);
    assertThat(configOrError.errorDetail).isNull();
    ExternalProcessorFilterConfig filterConfig = configOrError.config;

    // External Processor Server
    final AtomicReference<StreamObserver<ProcessingResponse>> responseObserverRef = new AtomicReference<>();
    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          @SuppressWarnings("unchecked")
          public StreamObserver<ProcessingRequest> process(
              final StreamObserver<ProcessingResponse> responseObserver) {
            responseObserverRef.set(responseObserver);
            ((ServerCallStreamObserver<ProcessingResponse>) responseObserver).request(100);
            return new StreamObserver<ProcessingRequest>() {
              @Override
              public void onNext(ProcessingRequest request) {
                if (request.hasRequestHeaders()) {
                  responseObserver.onNext(ProcessingResponse.newBuilder()
                      .setRequestHeaders(HeadersResponse.newBuilder().build())
                      .build());
                }
              }

              @Override public void onError(Throwable t) {}
              @Override public void onCompleted() { responseObserver.onCompleted(); }
            };
          }
        };
    grpcCleanup.register(InProcessServerBuilder.forName(uniqueExtProcServerName)
        .addService(extProcImpl)
        .directExecutor()
        .build().start());

    final AtomicBoolean sidecarReady = new AtomicBoolean(true);
    final AtomicReference<ClientCall.Listener<ProcessingResponse>> sidecarListenerRef =
        new AtomicReference<>();
    CachedChannelManager channelManager = new CachedChannelManager(config -> {
      return grpcCleanup.register(
          InProcessChannelBuilder.forName(uniqueExtProcServerName)
              .directExecutor()
              .intercept(new io.grpc.ClientInterceptor() {
                @Override
                public <ReqT, RespT> io.grpc.ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method, io.grpc.CallOptions callOptions, io.grpc.Channel next) {
                  return new io.grpc.ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                      next.newCall(method, callOptions)) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                      sidecarListenerRef.set((Listener<ProcessingResponse>) responseListener);
                      super.start(responseListener, headers);
                    }

                    @Override
                    public boolean isReady() {
                      return sidecarReady.get();
                    }
                  };
                }
              })
              .build());
    });

    ExternalProcessorServerInterceptor interceptor = new ExternalProcessorServerInterceptor(
        filterConfig, channelManager, FAKE_CONTEXT);

    final AtomicReference<ServerCall<InputStream, InputStream>> interceptedCallRef = new AtomicReference<>();
    ServerCallHandler<InputStream, InputStream> nextHandler = (call, headers) -> {
      interceptedCallRef.set(call);
      return new ServerCall.Listener<InputStream>() {};
    };

    final AtomicInteger rawRequestCount = new AtomicInteger(0);
    ServerCall<InputStream, InputStream> rawCall = new SimpleServerCall(METHOD_SAY_HELLO_RAW) {
      @Override
      public void request(int numMessages) {
        rawRequestCount.addAndGet(numMessages);
      }
    };

    try {
      interceptor.interceptCall(rawCall, new Metadata(), nextHandler);

      // Wait for sidecar call to start and listener to be captured
      long startTime = System.currentTimeMillis();
      while (sidecarListenerRef.get() == null && System.currentTimeMillis() - startTime < 5000) {
        Thread.sleep(10);
      }
      assertThat(sidecarListenerRef.get()).isNotNull();

      // Wait for activation
      startTime = System.currentTimeMillis();
      while (!interceptedCallRef.get().isReady() && System.currentTimeMillis() - startTime < 5000) {
        Thread.sleep(10);
      }
      assertThat(interceptedCallRef.get().isReady()).isTrue();

      // Sidecar is busy
      sidecarReady.set(false);
      assertThat(interceptedCallRef.get().isReady()).isFalse();

      // Application requests more messages
      interceptedCallRef.get().request(5);

      // Verify raw call NOT requested yet
      assertThat(rawRequestCount.get()).isEqualTo(0);

      // Sidecar becomes ready
      sidecarReady.set(true);
      sidecarListenerRef.get().onReady();

      // After sidecar becomes ready, pending requests should be drained to raw call.
      long start = System.currentTimeMillis();
      while (rawRequestCount.get() < 5 && System.currentTimeMillis() - start < 2000) {
        Thread.sleep(10);
      }
      assertThat(rawRequestCount.get()).isEqualTo(5);
      assertThat(interceptedCallRef.get().isReady()).isTrue();
    } finally {
      if (responseObserverRef.get() != null) {
        try {
          responseObserverRef.get().onCompleted();
        } catch (IllegalStateException ignored) {
          // Ignore if already closed
        }
      }
      channelManager.close();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void givenRequestDrainActive_whenAppRequestsMessages_thenRequestsBuffered()
      throws Exception {
    String uniqueExtProcServerName = InProcessServerBuilder.generateName();
    ExternalProcessor proto = ExternalProcessor.newBuilder()
        .setGrpcService(GrpcService.newBuilder()
            .setGoogleGrpc(GrpcService.GoogleGrpc.newBuilder()
                .setTargetUri("in-process:///" + uniqueExtProcServerName)
                .addChannelCredentialsPlugin(Any.newBuilder()
                    .setTypeUrl("type.googleapis.com/envoy.extensions.grpc_service." 
                        + "channel_credentials.insecure.v3.InsecureCredentials")
                    .build())
                .build())
            .build())
        .build();
    ConfigOrError<ExternalProcessorFilterConfig> configOrError =
        provider.parseFilterConfig(Any.pack(proto), filterContext);
    assertThat(configOrError.errorDetail).isNull();
    ExternalProcessorFilterConfig filterConfig = configOrError.config;

    final CountDownLatch drainLatch = new CountDownLatch(1);
    final AtomicReference<StreamObserver<ProcessingResponse>> responseObserverRef = new AtomicReference<>();
    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          @SuppressWarnings("unchecked")
          public StreamObserver<ProcessingRequest> process(
              final StreamObserver<ProcessingResponse> responseObserver) {
            responseObserverRef.set(responseObserver);
            ((ServerCallStreamObserver<ProcessingResponse>) responseObserver).request(100);
            return new StreamObserver<ProcessingRequest>() {
              @Override
              public void onNext(ProcessingRequest request) {
                if (request.hasRequestHeaders()) {
                  responseObserver.onNext(ProcessingResponse.newBuilder()
                      .setRequestDrain(true)
                      .build());
                  drainLatch.countDown();
                }
              }

              @Override public void onError(Throwable t) {}
              @Override public void onCompleted() {}
            };
          }
        };
    grpcCleanup.register(InProcessServerBuilder.forName(uniqueExtProcServerName)
        .addService(extProcImpl)
        .directExecutor()
        .build().start());

    CachedChannelManager channelManager = new CachedChannelManager(config -> {
      return grpcCleanup.register(
          InProcessChannelBuilder.forName(uniqueExtProcServerName).directExecutor().build());
    });

    ExternalProcessorServerInterceptor interceptor = new ExternalProcessorServerInterceptor(
        filterConfig, channelManager, FAKE_CONTEXT);

    final AtomicReference<ServerCall<InputStream, InputStream>> interceptedCallRef = new AtomicReference<>();
    ServerCallHandler<InputStream, InputStream> nextHandler = (call, headers) -> {
      interceptedCallRef.set(call);
      return new ServerCall.Listener<InputStream>() {};
    };

    final AtomicInteger rawRequestCount = new AtomicInteger(0);
    ServerCall<InputStream, InputStream> rawCall = new SimpleServerCall(METHOD_SAY_HELLO_RAW) {
      @Override
      public void request(int numMessages) {
        rawRequestCount.addAndGet(numMessages);
      }
    };

    try {
      interceptor.interceptCall(rawCall, new Metadata(), nextHandler);

      assertThat(drainLatch.await(5, TimeUnit.SECONDS)).isTrue();

      // Wait for interceptedCallRef to become ready first (it will transition to activated, then draining)
      long start = System.currentTimeMillis();
      while (interceptedCallRef.get() == null && System.currentTimeMillis() - start < 5000) {
        Thread.sleep(10);
      }
      assertThat(interceptedCallRef.get()).isNotNull();

      // isReady() must return false during drain.
      start = System.currentTimeMillis();
      while (interceptedCallRef.get().isReady() && System.currentTimeMillis() - start < 2000) {
        Thread.sleep(10);
      }
      assertThat(interceptedCallRef.get().isReady()).isFalse();

      // App requests more messages
      interceptedCallRef.get().request(3);

      // isReady() should remain false during drain
      assertThat(interceptedCallRef.get().isReady()).isFalse();

      // Verify raw call NOT requested during drain
      assertThat(rawRequestCount.get()).isEqualTo(0);
    } finally {
      if (responseObserverRef.get() != null) {
        try {
          responseObserverRef.get().onCompleted();
        } catch (IllegalStateException ignored) {
          // Ignore if already closed
        }
      }
      channelManager.close();
    }
  }


  // ============================================================================
  // Category 9: Error Handling & Security
  // ============================================================================

  @Test
  public void serverInterceptor_failOpen_allowsCallToProceed() throws Exception {
    ExternalProcessor proto = createBaseProto(extProcServerName)
        .setFailureModeAllow(true)
        .build();
    ConfigOrError<ExternalProcessorFilterConfig> configOrError =
        provider.parseFilterConfig(Any.pack(proto), filterContext);
    assertThat(configOrError.errorDetail).isNull();
    ExternalProcessorFilterConfig filterConfig = configOrError.config;

    // External processor returns immediate error
    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          public StreamObserver<ProcessingRequest> process(
              StreamObserver<ProcessingResponse> responseObserver) {
            responseObserver.onError(Status.UNAVAILABLE.withDescription("ExtProc down").asRuntimeException());
            return new StreamObserver<ProcessingRequest>() {
              @Override public void onNext(ProcessingRequest request) {}
              @Override public void onError(Throwable t) {}
              @Override public void onCompleted() {}
            };
          }
        };

    grpcCleanup.register(InProcessServerBuilder.forName(extProcServerName)
        .addService(extProcImpl)
        .directExecutor()
        .build().start());

    CachedChannelManager channelManager = new CachedChannelManager(config -> {
      return grpcCleanup.register(
          InProcessChannelBuilder.forName(extProcServerName)
              .directExecutor()
              .build());
    });

    ExternalProcessorServerInterceptor interceptor = new ExternalProcessorServerInterceptor(
        filterConfig, channelManager, FAKE_CONTEXT);

    final AtomicBoolean callStarted = new AtomicBoolean(false);
    dataPlaneHandler = new DataPlaneServiceHandler() {
      @Override
      public void sayHello(InputStream request, StreamObserver<InputStream> responseObserver) {
        callStarted.set(true);
        responseObserver.onNext(request);
        responseObserver.onCompleted();
      }
    };

    startDataPlane(interceptor);

    io.grpc.ClientCall<InputStream, InputStream> clientCall = dataPlaneChannel.newCall(
        METHOD_SAY_HELLO_RAW, io.grpc.CallOptions.DEFAULT);

    final CountDownLatch callCompletedLatch = new CountDownLatch(1);
    clientCall.start(new io.grpc.ClientCall.Listener<InputStream>() {
      @Override
      public void onClose(Status status, Metadata trailers) {
        callCompletedLatch.countDown();
      }
    }, new Metadata());

    clientCall.request(1);
    clientCall.sendMessage(new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)));
    clientCall.halfClose();

    assertThat(callCompletedLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(callStarted.get()).isTrue();
  }

  // ============================================================================
  // Category 10: Resource Management
  // ============================================================================

  @Test
  public void givenFilter_whenClosed_thenCachedChannelManagerIsClosed() throws Exception {
    CachedChannelManager mockChannelManager = Mockito.mock(CachedChannelManager.class);
    ExternalProcessorFilter filter = new ExternalProcessorFilter(FAKE_CONTEXT, mockChannelManager);
    filter.close();
    Mockito.verify(mockChannelManager).close();
  }

  // ============================================================================
  // Category 11: Data plane rpc cancellation
  // ============================================================================

  @Test
  @SuppressWarnings("unchecked")
  public void givenActiveRpc_whenDataPlaneCallCancelled_thenExtProcStreamIsErrored()
      throws Exception {
    String uniqueExtProcServerName = InProcessServerBuilder.generateName();
    ExternalProcessor proto = ExternalProcessor.newBuilder()
        .setGrpcService(GrpcService.newBuilder()
            .setGoogleGrpc(GrpcService.GoogleGrpc.newBuilder()
                .setTargetUri("in-process:///" + uniqueExtProcServerName)
                .addChannelCredentialsPlugin(Any.newBuilder()
                    .setTypeUrl("type.googleapis.com/envoy.extensions.grpc_service." 
                        + "channel_credentials.insecure.v3.InsecureCredentials")
                    .build())
                .build())
            .build())
        .build();
    ConfigOrError<ExternalProcessorFilterConfig> configOrError =
        provider.parseFilterConfig(Any.pack(proto), filterContext);
    assertThat(configOrError.errorDetail).isNull();
    ExternalProcessorFilterConfig filterConfig = configOrError.config;

    final CountDownLatch cancelLatch = new CountDownLatch(1);
    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          @SuppressWarnings("unchecked")
          public StreamObserver<ProcessingRequest> process(
              final StreamObserver<ProcessingResponse> responseObserver) {
            ((ServerCallStreamObserver<ProcessingResponse>) responseObserver).request(100);
            return new StreamObserver<ProcessingRequest>() {
              @Override public void onNext(ProcessingRequest request) {}

              @Override
              public void onError(Throwable t) {
                cancelLatch.countDown();
              }

              @Override public void onCompleted() {}
            };
          }
        };
    grpcCleanup.register(InProcessServerBuilder.forName(uniqueExtProcServerName)
        .addService(extProcImpl)
        .directExecutor()
        .build().start());

    CachedChannelManager channelManager = new CachedChannelManager(config -> {
      return grpcCleanup.register(
          InProcessChannelBuilder.forName(uniqueExtProcServerName).directExecutor().build());
    });

    ExternalProcessorServerInterceptor interceptor = new ExternalProcessorServerInterceptor(
        filterConfig, channelManager, FAKE_CONTEXT);

    final AtomicReference<ServerCall.Listener<InputStream>> interceptedListenerRef = new AtomicReference<>();
    ServerCallHandler<InputStream, InputStream> nextHandler = (call, headers) -> {
      ServerCall.Listener<InputStream> listener = new ServerCall.Listener<InputStream>() {};
      interceptedListenerRef.set(listener);
      return listener;
    };

    ServerCall<InputStream, InputStream> rawCall = new SimpleServerCall(METHOD_SAY_HELLO_RAW);

    try {
      ServerCall.Listener<InputStream> listener =
          interceptor.interceptCall(rawCall, new Metadata(), nextHandler);

      // Client cancels the RPC
      listener.onCancel();

      // Verify sidecar control stream also received onError/cancellation
      assertThat(cancelLatch.await(5, TimeUnit.SECONDS)).isTrue();
    } finally {
      channelManager.close();
    }
  }

  // ============================================================================
  // Category 12: Flow Control when side stream is full [SKIPPED]
  // ============================================================================

  // ============================================================================
  // Category 13: Streaming Completeness (Client & Bi-Di) [SKIPPED]
  // ============================================================================

  // ============================================================================
  // Category 14: Header Forwarding Rules [SKIPPED]
  // ============================================================================

  // ============================================================================
  // Category 15: Request Attributes [SKIPPED]
  // ============================================================================

  // ============================================================================
  // Category 16: Response Trailers
  // ============================================================================

  @Test
  public void serverInterceptor_responseTrailerMutation_mutatesTrailer() throws Exception {
    ExternalProcessor proto = createBaseProto(extProcServerName)
        .setProcessingMode(ProcessingMode.newBuilder()
            .setResponseHeaderMode(ProcessingMode.HeaderSendMode.SKIP)
            .setResponseTrailerMode(ProcessingMode.HeaderSendMode.SEND)
            .build())
        .build();
    ConfigOrError<ExternalProcessorFilterConfig> configOrError =
        provider.parseFilterConfig(Any.pack(proto), filterContext);
    assertThat(configOrError.errorDetail).isNull();
    ExternalProcessorFilterConfig filterConfig = configOrError.config;

    final CountDownLatch extProcLatch = new CountDownLatch(2); // 1 for request headers, 1 for response trailers
    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          @SuppressWarnings("unchecked")
          public StreamObserver<ProcessingRequest> process(
              StreamObserver<ProcessingResponse> responseObserver) {
            ((ServerCallStreamObserver<ProcessingResponse>) responseObserver).request(100);
            return new StreamObserver<ProcessingRequest>() {
              @Override
              public void onNext(ProcessingRequest request) {
                if (request.hasRequestHeaders()) {
                  responseObserver.onNext(ProcessingResponse.newBuilder()
                      .setRequestHeaders(HeadersResponse.newBuilder()
                          .setResponse(CommonResponse.newBuilder().build())
                          .build())
                      .build());
                  extProcLatch.countDown();
                } else if (request.hasResponseTrailers()) {
                  responseObserver.onNext(ProcessingResponse.newBuilder()
                      .setResponseTrailers(TrailersResponse.newBuilder()
                          .setHeaderMutation(HeaderMutation.newBuilder()
                              .addSetHeaders(io.envoyproxy.envoy.config.core.v3.HeaderValueOption.newBuilder()
                                  .setHeader(io.envoyproxy.envoy.config.core.v3.HeaderValue.newBuilder()
                                      .setKey("x-mutated-trailer")
                                      .setValue("mutated-trailer-value")
                                      .build())
                                  .build())
                              .build())
                          .build())
                      .build());
                  responseObserver.onCompleted();
                  extProcLatch.countDown();
                }
              }

              @Override public void onError(Throwable t) {}
              @Override public void onCompleted() {}
            };
          }
        };

    grpcCleanup.register(InProcessServerBuilder.forName(extProcServerName)
        .addService(extProcImpl)
        .directExecutor()
        .build().start());

    CachedChannelManager channelManager = new CachedChannelManager(config -> {
      return grpcCleanup.register(
          InProcessChannelBuilder.forName(extProcServerName)
              .directExecutor()
              .build());
    });

    ExternalProcessorServerInterceptor interceptor = new ExternalProcessorServerInterceptor(
        filterConfig, channelManager, FAKE_CONTEXT);

    dataPlaneHandler = new DataPlaneServiceHandler() {
      @Override
      public void sayHello(InputStream request, StreamObserver<InputStream> responseObserver) {
        responseObserver.onNext(request);
        responseObserver.onCompleted();
      }
    };

    startDataPlane(interceptor);

    io.grpc.ClientCall<InputStream, InputStream> clientCall = dataPlaneChannel.newCall(
        METHOD_SAY_HELLO_RAW, io.grpc.CallOptions.DEFAULT);

    final AtomicReference<Metadata> receivedTrailers = new AtomicReference<>();
    final CountDownLatch callCompletedLatch = new CountDownLatch(1);
    clientCall.start(new io.grpc.ClientCall.Listener<InputStream>() {
      @Override
      public void onClose(Status status, Metadata trailers) {
        receivedTrailers.set(trailers);
        callCompletedLatch.countDown();
      }
    }, new Metadata());

    clientCall.request(1);
    clientCall.sendMessage(new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)));
    clientCall.halfClose();

    assertThat(extProcLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(callCompletedLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(receivedTrailers.get()).isNotNull();
    assertThat(receivedTrailers.get().get(Metadata.Key.of("x-mutated-trailer", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("mutated-trailer-value");
  }

  @Test
  public void serverInterceptor_responseTrailerModeSkip_doesNotSendResponseTrailers() throws Exception {
    ExternalProcessor proto = createBaseProto(extProcServerName)
        .setProcessingMode(ProcessingMode.newBuilder()
            .setResponseTrailerMode(ProcessingMode.HeaderSendMode.SKIP)
            .build())
        .build();
    ConfigOrError<ExternalProcessorFilterConfig> configOrError =
        provider.parseFilterConfig(Any.pack(proto), filterContext);
    assertThat(configOrError.errorDetail).isNull();
    ExternalProcessorFilterConfig filterConfig = configOrError.config;

    final CountDownLatch extProcLatch = new CountDownLatch(2); // 1 for request headers, 1 for response headers
    final AtomicBoolean responseTrailersReceived = new AtomicBoolean(false);
    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          @SuppressWarnings("unchecked")
          public StreamObserver<ProcessingRequest> process(
              StreamObserver<ProcessingResponse> responseObserver) {
            ((ServerCallStreamObserver<ProcessingResponse>) responseObserver).request(100);
            return new StreamObserver<ProcessingRequest>() {
              @Override
              public void onNext(ProcessingRequest request) {
                if (request.hasRequestHeaders()) {
                  responseObserver.onNext(ProcessingResponse.newBuilder()
                      .setRequestHeaders(HeadersResponse.newBuilder()
                          .setResponse(CommonResponse.newBuilder().build())
                          .build())
                      .build());
                  extProcLatch.countDown();
                } else if (request.hasResponseHeaders()) {
                  responseObserver.onNext(ProcessingResponse.newBuilder()
                      .setResponseHeaders(HeadersResponse.newBuilder()
                          .setResponse(CommonResponse.newBuilder().build())
                          .build())
                      .build());
                  extProcLatch.countDown();
                } else if (request.hasResponseTrailers()) {
                  responseTrailersReceived.set(true);
                } else if (request.hasResponseBody()) {
                  responseObserver.onCompleted();
                }
              }

              @Override public void onError(Throwable t) {}
              @Override public void onCompleted() {}
            };
          }
        };

    grpcCleanup.register(InProcessServerBuilder.forName(extProcServerName)
        .addService(extProcImpl)
        .directExecutor()
        .build().start());

    CachedChannelManager channelManager = new CachedChannelManager(config -> {
      return grpcCleanup.register(
          InProcessChannelBuilder.forName(extProcServerName)
              .directExecutor()
              .build());
    });

    ExternalProcessorServerInterceptor interceptor = new ExternalProcessorServerInterceptor(
        filterConfig, channelManager, FAKE_CONTEXT);

    dataPlaneHandler = new DataPlaneServiceHandler() {
      @Override
      public void sayHello(InputStream request, StreamObserver<InputStream> responseObserver) {
        responseObserver.onNext(request);
        responseObserver.onCompleted();
      }
    };

    startDataPlane(interceptor);

    io.grpc.ClientCall<InputStream, InputStream> clientCall = dataPlaneChannel.newCall(
        METHOD_SAY_HELLO_RAW, io.grpc.CallOptions.DEFAULT);

    final AtomicReference<Metadata> receivedTrailers = new AtomicReference<>();
    final CountDownLatch callCompletedLatch = new CountDownLatch(1);
    clientCall.start(new io.grpc.ClientCall.Listener<InputStream>() {
      @Override
      public void onClose(Status status, Metadata trailers) {
        receivedTrailers.set(trailers);
        callCompletedLatch.countDown();
      }
    }, new Metadata());

    clientCall.request(1);
    clientCall.sendMessage(new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)));
    clientCall.halfClose();

    assertThat(extProcLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(callCompletedLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(responseTrailersReceived.get()).isFalse();
    assertThat(receivedTrailers.get()).isNotNull();
  }

  // ============================================================================
  // Category 17: Trailers-Only Response [SKIPPED]
  // ============================================================================

  // ============================================================================
  // Category 18: Response Ordering Checks [SKIPPED]
  // ============================================================================

  // ============================================================================
  // Category 19: Header Response Status Checks [SKIPPED]
  // ============================================================================

  // ============================================================================
  // Category 20: Concurrency and Thread Safety (Serialization)
  // ============================================================================

  @Test
  public void serverInterceptor_concurrency_serializesDelegateCallbacks() throws Exception {
    ExternalProcessor proto = createBaseProto(extProcServerName)
        .setProcessingMode(ProcessingMode.newBuilder()
            .setRequestBodyMode(ProcessingMode.BodySendMode.GRPC)
            .build())
        .build();
    ConfigOrError<ExternalProcessorFilterConfig> configOrError =
        provider.parseFilterConfig(Any.pack(proto), filterContext);
    assertThat(configOrError.errorDetail).isNull();
    ExternalProcessorFilterConfig filterConfig = configOrError.config;

    final int ITERATIONS = 500;
    final CountDownLatch clientDone = new CountDownLatch(1);
    final AtomicBoolean raceDetected = new AtomicBoolean(false);
    final AtomicInteger activeServiceCalls = new AtomicInteger(0);

    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          public StreamObserver<ProcessingRequest> process(
              StreamObserver<ProcessingResponse> responseObserver) {
            ((ServerCallStreamObserver<ProcessingResponse>) responseObserver).request(100);
            return new StreamObserver<ProcessingRequest>() {
              @Override
              public void onNext(ProcessingRequest request) {
                if (request.hasRequestHeaders()) {
                  responseObserver.onNext(ProcessingResponse.newBuilder()
                      .setRequestHeaders(HeadersResponse.newBuilder()
                          .setResponse(CommonResponse.newBuilder().build())
                          .build())
                      .build());
                } else if (request.hasRequestBody()) {
                  boolean eos = request.getRequestBody().getEndOfStream();
                  BodyResponse.Builder bodyResponseBuilder = BodyResponse.newBuilder();
                  if (eos) {
                    bodyResponseBuilder.setResponse(CommonResponse.newBuilder()
                        .setBodyMutation(BodyMutation.newBuilder()
                            .setStreamedResponse(StreamedBodyResponse.newBuilder()
                                .setEndOfStream(true)
                                .build())
                            .build())
                        .build());
                  } else {
                    ByteString body = request.getRequestBody().getBody();
                    bodyResponseBuilder.setResponse(CommonResponse.newBuilder()
                        .setBodyMutation(BodyMutation.newBuilder()
                            .setStreamedResponse(StreamedBodyResponse.newBuilder()
                                .setBody(body)
                                .build())
                            .build())
                        .build());
                  }
                  responseObserver.onNext(ProcessingResponse.newBuilder()
                      .setRequestBody(bodyResponseBuilder.build())
                      .build());
                } else if (request.hasResponseHeaders()) {
                  responseObserver.onNext(ProcessingResponse.newBuilder()
                      .setResponseHeaders(HeadersResponse.newBuilder()
                          .setResponse(CommonResponse.newBuilder().build())
                          .build())
                      .build());
                } else if (request.hasResponseBody()) {
                  boolean eos = request.getResponseBody().getEndOfStream();
                  BodyResponse.Builder bodyResponseBuilder = BodyResponse.newBuilder();
                  if (eos) {
                    bodyResponseBuilder.setResponse(CommonResponse.newBuilder()
                        .setBodyMutation(BodyMutation.newBuilder()
                            .setStreamedResponse(StreamedBodyResponse.newBuilder()
                                .setEndOfStream(true)
                                .build())
                            .build())
                        .build());
                  } else {
                    ByteString body = request.getResponseBody().getBody();
                    bodyResponseBuilder.setResponse(CommonResponse.newBuilder()
                        .setBodyMutation(BodyMutation.newBuilder()
                            .setStreamedResponse(StreamedBodyResponse.newBuilder()
                                .setBody(body)
                                .build())
                            .build())
                        .build());
                  }
                  responseObserver.onNext(ProcessingResponse.newBuilder()
                      .setResponseBody(bodyResponseBuilder.build())
                      .build());
                }
              }
              @Override public void onError(Throwable t) {}
              @Override public void onCompleted() {
                responseObserver.onCompleted();
              }
            };
          }
        };

    grpcCleanup.register(InProcessServerBuilder.forName(extProcServerName)
        .addService(extProcImpl)
        .directExecutor()
        .build().start());

    CachedChannelManager channelManager = new CachedChannelManager(config -> {
      return grpcCleanup.register(
          InProcessChannelBuilder.forName(extProcServerName)
              .directExecutor()
              .build());
    });

    ExternalProcessorServerInterceptor interceptor = new ExternalProcessorServerInterceptor(
        filterConfig, channelManager, FAKE_CONTEXT);

    dataPlaneHandler = new DataPlaneServiceHandler() {
      @Override
      public StreamObserver<InputStream> sayHelloBidi(StreamObserver<InputStream> responseObserver) {
        return new StreamObserver<InputStream>() {
          @Override
          public void onNext(InputStream value) {
            int active = activeServiceCalls.incrementAndGet();
            if (active > 1) {
              raceDetected.set(true);
            }
            try {
              Thread.sleep(1);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            activeServiceCalls.decrementAndGet();
            responseObserver.onNext(value);
          }

          @Override public void onError(Throwable t) {
            responseObserver.onError(t);
          }
          @Override
          public void onCompleted() {
            responseObserver.onCompleted();
          }
        };
      }
    };

    startDataPlane(interceptor);

    io.grpc.ClientCall<InputStream, InputStream> clientCall = dataPlaneChannel.newCall(
        METHOD_SAY_HELLO_BIDI, io.grpc.CallOptions.DEFAULT);

    final CountDownLatch clientClosedLatch = new CountDownLatch(1);
    final AtomicInteger clientReceivedMessages = new AtomicInteger(0);

    clientCall.start(new io.grpc.ClientCall.Listener<InputStream>() {
      @Override
      public void onMessage(InputStream message) {
        clientReceivedMessages.incrementAndGet();
        clientCall.request(1);
      }
      @Override
      public void onClose(Status status, Metadata trailers) {
        clientClosedLatch.countDown();
      }
    }, new Metadata());

    clientCall.request(1);

    Thread clientThread = new Thread(() -> {
      try {
        for (int i = 0; i < ITERATIONS; i++) {
          clientCall.sendMessage(new ByteArrayInputStream(new byte[10]));
          Thread.sleep(0, 10000);
        }
        clientCall.halfClose();
        clientDone.countDown();
      } catch (Exception e) {
        e.printStackTrace();
        raceDetected.set(true);
      }
    });

    clientThread.start();
    clientThread.join();

    assertThat(clientDone.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(clientClosedLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(clientReceivedMessages.get()).isEqualTo(ITERATIONS);
    assertThat(raceDetected.get()).isFalse();
  }

  @Test
  public void serverInterceptor_outboundStreamTermination_serializesSendMessage() throws Exception {
    // Configure response body mode to GRPC
    ExternalProcessor proto = createBaseProto(extProcServerName)
        .setFailureModeAllow(true)
        .setProcessingMode(ProcessingMode.newBuilder()
            .setResponseBodyMode(ProcessingMode.BodySendMode.GRPC)
            .build())
        .build();
    ConfigOrError<ExternalProcessorFilterConfig> configOrError =
        provider.parseFilterConfig(Any.pack(proto), filterContext);
    assertThat(configOrError.errorDetail).isNull();
    ExternalProcessorFilterConfig filterConfig = configOrError.config;

    final AtomicReference<StreamObserver<ProcessingResponse>> responseObserverRef = new AtomicReference<>();
    final CountDownLatch streamActiveLatch = new CountDownLatch(1);

    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          public StreamObserver<ProcessingRequest> process(
              StreamObserver<ProcessingResponse> responseObserver) {
            responseObserverRef.set(responseObserver);
            streamActiveLatch.countDown();
            return new StreamObserver<ProcessingRequest>() {
              @Override public void onNext(ProcessingRequest request) {}
              @Override public void onError(Throwable t) {}
              @Override public void onCompleted() {}
            };
          }
        };

    grpcCleanup.register(InProcessServerBuilder.forName(extProcServerName)
        .addService(extProcImpl)
        .directExecutor()
        .build().start());

    CachedChannelManager channelManager = new CachedChannelManager(config -> {
      return grpcCleanup.register(
          InProcessChannelBuilder.forName(extProcServerName)
              .directExecutor()
              .build());
    });

    ExternalProcessorServerInterceptor interceptor = new ExternalProcessorServerInterceptor(
        filterConfig, channelManager, FAKE_CONTEXT);

    final AtomicReference<ServerCall<InputStream, InputStream>> wrappedCallRef = new AtomicReference<>();
    ServerInterceptor capturingInterceptor = new ServerInterceptor() {
      @SuppressWarnings("unchecked")
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
          ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        wrappedCallRef.set((ServerCall<InputStream, InputStream>) call);
        return next.startCall(call, headers);
      }
    };

    final AtomicReference<ConcurrencyDetectingServerCall> rawCallRef = new AtomicReference<>();
    ServerInterceptor capturingRawCallInterceptor = new ServerInterceptor() {
      @SuppressWarnings("unchecked")
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
          ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        ConcurrencyDetectingServerCall wrapped = new ConcurrencyDetectingServerCall(
            (ServerCall<InputStream, InputStream>) call);
        rawCallRef.set(wrapped);
        return next.startCall((ServerCall<ReqT, RespT>) (ServerCall<?, ?>) wrapped, headers);
      }
    };

    startDataPlane(capturingRawCallInterceptor, interceptor, capturingInterceptor);

    Metadata initialHeaders = new Metadata();
    io.grpc.ClientCall<InputStream, InputStream> clientCall = dataPlaneChannel.newCall(
        METHOD_SAY_HELLO_RAW, io.grpc.CallOptions.DEFAULT);

    final CountDownLatch callCompletedLatch = new CountDownLatch(1);
    clientCall.start(new io.grpc.ClientCall.Listener<InputStream>() {
      @Override
      public void onClose(Status status, Metadata trailers) {
        callCompletedLatch.countDown();
      }
    }, initialHeaders);

    clientCall.request(1);
    clientCall.sendMessage(new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)));
    clientCall.halfClose();

    boolean active = streamActiveLatch.await(5, TimeUnit.SECONDS);
    assertThat(active).isTrue();

    StreamObserver<ProcessingResponse> responseObserver = responseObserverRef.get();
    responseObserver.onNext(ProcessingResponse.newBuilder()
        .setRequestHeaders(HeadersResponse.newBuilder()
            .setResponse(CommonResponse.newBuilder().build())
            .build())
        .build());

    ServerCall<InputStream, InputStream> wrappedCall = wrappedCallRef.get();
    assertThat(wrappedCall).isNotNull();
    wrappedCall.sendHeaders(new Metadata());

    ConcurrencyDetectingServerCall rawCall = rawCallRef.get();
    assertThat(rawCall).isNotNull();

    responseObserver.onError(new RuntimeException("Stream dropped"));

    final CountDownLatch messageSentLatch = new CountDownLatch(2);
    Thread appThread = new Thread(() -> {
      try {
        wrappedCall.sendMessage(new ByteArrayInputStream("app-msg".getBytes(StandardCharsets.UTF_8)));
        messageSentLatch.countDown();
      } catch (Exception e) {
      }
    });

    wrappedCall.sendMessage(new ByteArrayInputStream("buffered-msg".getBytes(StandardCharsets.UTF_8)));
    messageSentLatch.countDown();

    appThread.start();
    appThread.join();

    boolean sent = messageSentLatch.await(5, TimeUnit.SECONDS);
    assertThat(sent).isTrue();
    assertThat(rawCall.concurrentCallDetected.get()).isFalse();
  }

  @Test
  public void serverInterceptor_concurrentSendHeadersAndFailOpen_flushesHeadersCorrectly() throws Exception {
    ExternalProcessor proto = createBaseProto(extProcServerName)
        .setFailureModeAllow(true)
        .build();
    ConfigOrError<ExternalProcessorFilterConfig> configOrError =
        provider.parseFilterConfig(Any.pack(proto), filterContext);
    assertThat(configOrError.errorDetail).isNull();
    ExternalProcessorFilterConfig filterConfig = configOrError.config;

    final AtomicReference<StreamObserver<ProcessingResponse>> responseObserverRef = new AtomicReference<>();
    final CountDownLatch streamActiveLatch = new CountDownLatch(1);

    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          public StreamObserver<ProcessingRequest> process(
              StreamObserver<ProcessingResponse> responseObserver) {
            responseObserverRef.set(responseObserver);
            streamActiveLatch.countDown();
            return new StreamObserver<ProcessingRequest>() {
              @Override public void onNext(ProcessingRequest request) {}
              @Override public void onError(Throwable t) {}
              @Override public void onCompleted() {}
            };
          }
        };

    grpcCleanup.register(InProcessServerBuilder.forName(extProcServerName)
        .addService(extProcImpl)
        .directExecutor()
        .build().start());

    CachedChannelManager channelManager = new CachedChannelManager(config -> {
      return grpcCleanup.register(
          InProcessChannelBuilder.forName(extProcServerName)
              .directExecutor()
              .build());
    });

    ExternalProcessorServerInterceptor interceptor = new ExternalProcessorServerInterceptor(
        filterConfig, channelManager, FAKE_CONTEXT);

    final AtomicReference<ServerCall<InputStream, InputStream>> wrappedCallRef = new AtomicReference<>();
    ServerInterceptor capturingInterceptor = new ServerInterceptor() {
      @SuppressWarnings("unchecked")
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
          ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        wrappedCallRef.set((ServerCall<InputStream, InputStream>) call);
        return next.startCall(call, headers);
      }
    };

    final AtomicReference<ConcurrencyDetectingServerCall> rawCallRef = new AtomicReference<>();
    ServerInterceptor capturingRawCallInterceptor = new ServerInterceptor() {
      @SuppressWarnings("unchecked")
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
          ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        ConcurrencyDetectingServerCall wrapped = new ConcurrencyDetectingServerCall(
            (ServerCall<InputStream, InputStream>) call);
        rawCallRef.set(wrapped);
        return next.startCall((ServerCall<ReqT, RespT>) (ServerCall<?, ?>) wrapped, headers);
      }
    };

    startDataPlane(capturingRawCallInterceptor, interceptor, capturingInterceptor);

    Metadata initialHeaders = new Metadata();
    io.grpc.ClientCall<InputStream, InputStream> clientCall = dataPlaneChannel.newCall(
        METHOD_SAY_HELLO_RAW, io.grpc.CallOptions.DEFAULT);

    final CountDownLatch headersReceivedLatch = new CountDownLatch(1);
    final AtomicReference<Metadata> receivedResponseHeadersRef = new AtomicReference<>();
    clientCall.start(new io.grpc.ClientCall.Listener<InputStream>() {
      @Override
      public void onHeaders(Metadata headers) {
        receivedResponseHeadersRef.set(headers);
        headersReceivedLatch.countDown();
      }
    }, initialHeaders);

    clientCall.request(1);
    clientCall.sendMessage(new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)));
    clientCall.halfClose();

    boolean active = streamActiveLatch.await(5, TimeUnit.SECONDS);
    assertThat(active).isTrue();

    StreamObserver<ProcessingResponse> responseObserver = responseObserverRef.get();
    responseObserver.onNext(ProcessingResponse.newBuilder()
        .setRequestHeaders(HeadersResponse.newBuilder()
            .setResponse(CommonResponse.newBuilder().build())
            .build())
        .build());

    ServerCall<InputStream, InputStream> wrappedCall = wrappedCallRef.get();
    assertThat(wrappedCall).isNotNull();

    ConcurrencyDetectingServerCall rawCall = rawCallRef.get();
    assertThat(rawCall).isNotNull();

    Thread appThread = new Thread(() -> {
      Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("x-resp-header", Metadata.ASCII_STRING_MARSHALLER), "val");
      wrappedCall.sendHeaders(headers);
      wrappedCall.close(Status.OK, new Metadata());
    });

    appThread.start();
    responseObserver.onError(new RuntimeException("Stream failure"));

    appThread.join();

    boolean headersSent = headersReceivedLatch.await(5, TimeUnit.SECONDS);
    assertThat(headersSent).isTrue();
    assertThat(receivedResponseHeadersRef.get().get(
        Metadata.Key.of("x-resp-header", Metadata.ASCII_STRING_MARSHALLER))).isEqualTo("val");
    assertThat(rawCall.concurrentCallDetected.get()).isFalse();
  }
}
