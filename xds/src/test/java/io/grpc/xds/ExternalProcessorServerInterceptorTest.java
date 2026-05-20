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
import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.internal.FakeClock;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.xds.ExternalProcessorFilter.ExternalProcessorFilterConfig;
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

    ServerCall<InputStream, InputStream> dummyCall = new ServerCall<InputStream, InputStream>() {
      @Override
      public void request(int numMessages) {}
      @Override
      public void sendHeaders(Metadata headers) {}
      @Override
      public void sendMessage(InputStream message) {}
      @Override
      public void close(Status status, Metadata trailers) {}
      @Override
      public boolean isCancelled() { return false; }
      @Override
      public MethodDescriptor<InputStream, InputStream> getMethodDescriptor() {
        return METHOD_SAY_HELLO_RAW;
      }
    };

    final AtomicReference<Metadata> receivedHeaders = new AtomicReference<>();
    ServerCallHandler<InputStream, InputStream> dummyNext = new ServerCallHandler<InputStream, InputStream>() {
      @Override
      public ServerCall.Listener<InputStream> startCall(ServerCall<InputStream, InputStream> call, Metadata headers) {
        receivedHeaders.set(headers);
        return new ServerCall.Listener<InputStream>() {};
      }
    };

    Metadata initialHeaders = new Metadata();
    initialHeaders.put(Metadata.Key.of("x-initial-header", Metadata.ASCII_STRING_MARSHALLER), "initial-value");

    interceptor.interceptCall(dummyCall, initialHeaders, dummyNext);

    assertThat(extProcLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(receivedHeaders.get()).isNotNull();
    assertThat(receivedHeaders.get().get(Metadata.Key.of("x-mutated-header", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("mutated-value");
  }

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

    final AtomicBoolean callStarted = new AtomicBoolean(false);
    ServerCallHandler<InputStream, InputStream> dummyNext = new ServerCallHandler<InputStream, InputStream>() {
      @Override
      public ServerCall.Listener<InputStream> startCall(ServerCall<InputStream, InputStream> call, Metadata headers) {
        callStarted.set(true);
        return new ServerCall.Listener<InputStream>() {};
      }
    };

    interceptor.interceptCall(dummyCall, new Metadata(), dummyNext);

    assertThat(callStarted.get()).isTrue();
  }

  @Test
  public void serverInterceptor_concurrency_serializesDelegateCallbacks() throws Exception {
    ExternalProcessor proto = createBaseProto(extProcServerName).build();
    ConfigOrError<ExternalProcessorFilterConfig> configOrError =
        provider.parseFilterConfig(Any.pack(proto), filterContext);
    assertThat(configOrError.errorDetail).isNull();
    ExternalProcessorFilterConfig filterConfig = configOrError.config;

    final AtomicReference<StreamObserver<ProcessingResponse>> responseObserverRef = new AtomicReference<>();
    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          public StreamObserver<ProcessingRequest> process(
              StreamObserver<ProcessingResponse> responseObserver) {
            responseObserverRef.set(responseObserver);
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

    final AtomicInteger concurrentCalls = new AtomicInteger(0);
    final AtomicBoolean raceDetected = new AtomicBoolean(false);
    final int ITERATIONS = 1000;

    ServerCall.Listener<InputStream> delegateListener = new ServerCall.Listener<InputStream>() {
      @Override
      public void onMessage(InputStream message) {
        int current = concurrentCalls.incrementAndGet();
        if (current > 1) {
          raceDetected.set(true);
        }
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        concurrentCalls.decrementAndGet();
      }
    };

    ServerCallHandler<InputStream, InputStream> dummyNext = new ServerCallHandler<InputStream, InputStream>() {
      @Override
      public ServerCall.Listener<InputStream> startCall(ServerCall<InputStream, InputStream> call, Metadata headers) {
        return delegateListener;
      }
    };

    @SuppressWarnings("unchecked")
    ExternalProcessorServerInterceptor.DataPlaneServerListener serverListener =
        (ExternalProcessorServerInterceptor.DataPlaneServerListener)
            interceptor.interceptCall(dummyCall, new Metadata(), dummyNext);

    // Simulate that the call is active and pass-through mode is enabled for quick delegate execution
    serverListener.setDelegate(delegateListener);

    Thread threadA = new Thread(() -> {
      for (int i = 0; i < ITERATIONS; i++) {
        serverListener.onMessage(new ByteArrayInputStream(new byte[0]));
      }
    });

    Thread threadB = new Thread(() -> {
      for (int i = 0; i < ITERATIONS; i++) {
        serverListener.onExternalBody(ByteString.EMPTY);
      }
    });

    threadA.start();
    threadB.start();
    threadA.join();
    threadB.join();

    StreamObserver<ProcessingResponse> responseObserver = responseObserverRef.get();
    if (responseObserver != null) {
      responseObserver.onCompleted();
    }

    assertThat(raceDetected.get()).isFalse();
  }

  @Test
  public void serverInterceptor_grpcBodyMode_dispatchesMessageToExtProcImmediatelyDuringIdle() throws Exception {
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
    final AtomicReference<StreamObserver<ProcessingResponse>> responseObserverRef = new AtomicReference<>();
    final CountDownLatch requestLatch = new CountDownLatch(2); // Expect 1 for headers, 1 for body

    ExternalProcessorGrpc.ExternalProcessorImplBase extProcImpl =
        new ExternalProcessorGrpc.ExternalProcessorImplBase() {
          @Override
          public StreamObserver<ProcessingRequest> process(
              StreamObserver<ProcessingResponse> responseObserver) {
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
    ExternalProcessorServerInterceptor.DataPlaneServerListener serverListener =
        (ExternalProcessorServerInterceptor.DataPlaneServerListener)
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

    // Assert that the data plane call was not started yet
    assertThat(startCallCalled.get()).isFalse();

    // Clean up control stream to release gRPC channels cleanly
    StreamObserver<ProcessingResponse> responseObserver = responseObserverRef.get();
    if (responseObserver != null) {
      responseObserver.onCompleted();
    }

    // After control stream completes, it should trigger fail-open/fallback activation of the data plane call
    assertThat(startCallCalled.get()).isTrue();
  }
}
