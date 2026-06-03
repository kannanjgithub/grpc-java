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

import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.envoyproxy.envoy.config.core.v3.GrpcService;
import io.envoyproxy.envoy.extensions.filters.http.ext_proc.v3.ExtProcOverrides;
import io.envoyproxy.envoy.extensions.filters.http.ext_proc.v3.ExtProcPerRoute;
import io.envoyproxy.envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor;
import io.envoyproxy.envoy.extensions.filters.http.ext_proc.v3.HeaderForwardingRules;
import io.envoyproxy.envoy.extensions.filters.http.ext_proc.v3.ProcessingMode;
import io.envoyproxy.envoy.service.ext_proc.v3.BodyMutation;
import io.envoyproxy.envoy.service.ext_proc.v3.BodyResponse;
import io.envoyproxy.envoy.service.ext_proc.v3.CommonResponse;
import io.envoyproxy.envoy.service.ext_proc.v3.ExternalProcessorGrpc;
import io.envoyproxy.envoy.service.ext_proc.v3.HeaderMutation;
import io.envoyproxy.envoy.service.ext_proc.v3.HeadersResponse;
import io.envoyproxy.envoy.service.ext_proc.v3.ImmediateResponse;
import io.envoyproxy.envoy.service.ext_proc.v3.ProcessingRequest;
import io.envoyproxy.envoy.service.ext_proc.v3.ProcessingResponse;
import io.envoyproxy.envoy.service.ext_proc.v3.StreamedBodyResponse;
import io.envoyproxy.envoy.service.ext_proc.v3.TrailersResponse;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.NameResolverRegistry;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.internal.FakeClock;
import io.grpc.internal.GrpcUtil;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.util.MutableHandlerRegistry;
import io.grpc.xds.ExternalProcessorFilter.ExternalProcessorFilterConfig;
import io.grpc.xds.ExternalProcessorFilter.ExternalProcessorFilterOverrideConfig;
import io.grpc.xds.client.Bootstrapper;
import io.grpc.xds.client.EnvoyProtoData.Node;
import io.grpc.xds.internal.grpcservice.CachedChannelManager;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ExternalProcessorFilter}.
 */
@RunWith(JUnit4.class)
public class ExternalProcessorFilterTest {
  static {
    System.setProperty("GRPC_EXPERIMENTAL_XDS_EXT_PROC_ON_CLIENT", "true");
  }

  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private MutableHandlerRegistry dataPlaneServiceRegistry;

  private String dataPlaneServerName;
  private String extProcServerName;
  private ExternalProcessorFilter.Provider provider;
  private static final Filter.FilterContext FAKE_CONTEXT = Filter.FilterContext.create(
      "test-filter", new io.grpc.MetricRecorder() {});
  private Filter.FilterConfigParseContext filterContext;
  private Bootstrapper.BootstrapInfo bootstrapInfo;
  private Bootstrapper.ServerInfo serverInfo;

  private static class InProcessNameResolverProvider extends NameResolverProvider {
    @Override
    public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
      if ("in-process".equals(targetUri.getScheme())) {
        return new NameResolver() {
          @Override
          public String getServiceAuthority() {
            return "localhost";
          }

          @Override
          public void start(Listener2 listener) {
          }

          @Override
          public void shutdown() {
          }
        };
      }
      return null;
    }

    @Override
    protected boolean isAvailable() {
      return true;
    }

    @Override
    protected int priority() {
      return 5;
    }

    @Override
    public String getDefaultScheme() {
      return "in-process";
    }

    @Override
    public Collection<Class<? extends SocketAddress>> getProducedSocketAddressTypes() {
      return Collections.emptyList();
    }
  }

  @Before
  public void setUp() throws Exception {
    NameResolverRegistry.getDefaultRegistry().register(new InProcessNameResolverProvider());

    dataPlaneServiceRegistry = new MutableHandlerRegistry();
    dataPlaneServerName = InProcessServerBuilder.generateName();
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

    grpcCleanup.register(InProcessServerBuilder.forName(dataPlaneServerName)
        .fallbackHandlerRegistry(dataPlaneServiceRegistry)
        .directExecutor()
        .build().start());
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

  // --- Category 1: Configuration Parsing & Provider ---

  @Test
  public void givenValidConfig_whenParsed_thenReturnsFilterConfig() throws Exception {
    ExternalProcessor proto = createBaseProto(extProcServerName).build();

    ConfigOrError<ExternalProcessorFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), filterContext);

    assertThat(result.errorDetail).isNull();
    assertThat(result.config).isNotNull();
    assertThat(result.config.typeUrl()).isEqualTo(ExternalProcessorFilter.TYPE_URL);
  }

  @Test
  public void givenUnsupportedBodyMode_whenParsed_thenReturnsError() throws Exception {
    ExternalProcessor proto = createBaseProto(extProcServerName)
        .setProcessingMode(ProcessingMode.newBuilder()
            .setRequestBodyMode(ProcessingMode.BodySendMode.BUFFERED) // Unsupported
            .build())
        .build();

    ConfigOrError<ExternalProcessorFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), filterContext);

    assertThat(result.errorDetail).contains("Invalid request_body_mode");
  }

  @Test
  public void givenInvalidGrpcService_whenParsed_thenReturnsError() throws Exception {
    ExternalProcessor proto = ExternalProcessor.newBuilder()
        .setGrpcService(GrpcService.newBuilder().build()) // Invalid: no GoogleGrpc
        .build();

    ConfigOrError<ExternalProcessorFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), filterContext);

    assertThat(result.errorDetail).contains("GrpcService must have GoogleGrpc");
  }

  @Test
  public void givenInvalidDeferredCloseTimeout_whenParsed_thenReturnsError() throws Exception {
    ExternalProcessor proto = createBaseProto(extProcServerName)
        .setDeferredCloseTimeout(
            com.google.protobuf.Duration.newBuilder().setSeconds(315576000001L).build())
        .build();

    ConfigOrError<ExternalProcessorFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), filterContext);

    assertThat(result.errorDetail).contains("Invalid deferred_close_timeout");
  }

  @Test
  public void givenNegativeDeferredCloseTimeout_whenParsed_thenReturnsError() throws Exception {
    ExternalProcessor proto = createBaseProto(extProcServerName)
        .setDeferredCloseTimeout(
            com.google.protobuf.Duration.newBuilder().setSeconds(0).setNanos(0).build())
        .build();

    ConfigOrError<ExternalProcessorFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), filterContext);

    assertThat(result.errorDetail).contains("deferred_close_timeout must be positive");
  }


  @Test
  public void provider_registeredInFilterRegistry_basedOnFlag() {
    // Test with flag true
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

    // Test with flag false
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

  // --- Category 2: Configuration Override ---

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
    ExtProcPerRoute perRoute = ExtProcPerRoute.newBuilder()
        .setOverrides(ExtProcOverrides.newBuilder()
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

  @Test
  public void givenOverrideConfig_whenOverridesMissing_thenFallsBackToDefaultInstance()
      throws Exception {
    ExtProcPerRoute perRoute = ExtProcPerRoute.newBuilder().build();

    ConfigOrError<ExternalProcessorFilterOverrideConfig> overrideResult = 
        provider.parseFilterConfigOverride(Any.pack(perRoute), filterContext);
    assertThat(overrideResult.errorDetail).isNull();
    ExternalProcessorFilterOverrideConfig overrideConfig = overrideResult.config;

    assertThat(overrideConfig.hasProcessingMode()).isFalse();
    assertThat(overrideConfig.hasRequestAttributes()).isFalse();
    assertThat(overrideConfig.hasResponseAttributes()).isFalse();
    assertThat(overrideConfig.hasGrpcService()).isFalse();
    assertThat(overrideConfig.hasFailureModeAllow()).isFalse();
    assertThat(overrideConfig.getGrpcServiceConfig()).isNull();
  }

  @Test
  public void givenOverrideConfig_whenFailureModeAllowOverridden_thenTakesEffect()
      throws Exception {
    ExternalProcessor parentProto = createBaseProto(extProcServerName)
        .setFailureModeAllow(false)
        .build();
    ExtProcPerRoute perRoute = ExtProcPerRoute.newBuilder()
        .setOverrides(ExtProcOverrides.newBuilder()
            .setFailureModeAllow(com.google.protobuf.BoolValue.of(true))
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

    assertThat(interceptor.getFilterConfig().getFailureModeAllow()).isTrue();
  }



  @Test
  public void givenOverrideConfig_whenOtherFieldsOverridden_thenReplaced() throws Exception {
    ExternalProcessor parentProto = createBaseProto(extProcServerName)
        .addRequestAttributes("attr1")
        .addResponseAttributes("attr2")
        .setFailureModeAllow(false)
        .build();
    
    GrpcService overrideService = GrpcService.newBuilder()
        .setGoogleGrpc(GrpcService.GoogleGrpc.newBuilder()
            .setTargetUri("in-process:///overridden")
            .addChannelCredentialsPlugin(Any.newBuilder()
                .setTypeUrl("type.googleapis.com/envoy.extensions.grpc_service." 
                + "channel_credentials.insecure.v3.InsecureCredentials")
                .build())
            .build())
        .build();
    ExtProcPerRoute perRoute = ExtProcPerRoute.newBuilder()
        .setOverrides(ExtProcOverrides.newBuilder()
            .addRequestAttributes("attr3")
            .addResponseAttributes("attr4")
            .setGrpcService(overrideService)
            .setFailureModeAllow(com.google.protobuf.BoolValue.of(true))
            .build())
        .build();

    ConfigOrError<ExternalProcessorFilterConfig> parentResult = 
        provider.parseFilterConfig(Any.pack(parentProto), filterContext);
    ExternalProcessorFilterConfig parentConfig = parentResult.config;
    ConfigOrError<ExternalProcessorFilterOverrideConfig> overrideResult = 
        provider.parseFilterConfigOverride(Any.pack(perRoute), filterContext);
    ExternalProcessorFilterOverrideConfig overrideConfig = overrideResult.config;

    ExternalProcessorFilter filter = new ExternalProcessorFilter(FAKE_CONTEXT);
    ExternalProcessorServerInterceptor interceptor = (ExternalProcessorServerInterceptor)
        filter.buildServerInterceptor(parentConfig, overrideConfig);
    ExternalProcessor mergedProto = interceptor.getFilterConfig().getExternalProcessor();

    assertThat(mergedProto.getRequestAttributesList()).containsExactly("attr3");
    assertThat(mergedProto.getResponseAttributesList()).containsExactly("attr4");
    assertThat(mergedProto.getGrpcService()).isEqualTo(overrideService);
    assertThat(interceptor.getFilterConfig().getFailureModeAllow()).isTrue();
  }

  @Test
  public void givenOverrideConfig_whenProcessingModeOverridden_thenReplacesWholeMode()
      throws Exception {
    ExternalProcessor parentProto = createBaseProto(extProcServerName)
        .setProcessingMode(ProcessingMode.newBuilder()
            .setRequestHeaderMode(ProcessingMode.HeaderSendMode.SKIP)
            .setRequestBodyMode(ProcessingMode.BodySendMode.NONE)
            .setResponseHeaderMode(ProcessingMode.HeaderSendMode.SKIP)
            .setResponseBodyMode(ProcessingMode.BodySendMode.GRPC)
            .setResponseTrailerMode(ProcessingMode.HeaderSendMode.SEND).build())
        .build();
    ExtProcPerRoute perRoute = ExtProcPerRoute.newBuilder()
        .setOverrides(ExtProcOverrides.newBuilder()
            .setProcessingMode(ProcessingMode.newBuilder()
                .setRequestBodyMode(ProcessingMode.BodySendMode.GRPC).build())
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

    ProcessingMode mergedMode = 
        interceptor.getFilterConfig().getExternalProcessor().getProcessingMode();
    // Full replacement: requestBodyMode becomes GRPC, others become defaults (0/DEFAULT/NONE)
    assertThat(mergedMode.getRequestBodyMode()).isEqualTo(ProcessingMode.BodySendMode.GRPC);
    assertThat(mergedMode.getRequestHeaderMode()).isEqualTo(ProcessingMode.HeaderSendMode.DEFAULT);
    assertThat(mergedMode.getResponseHeaderMode()).isEqualTo(ProcessingMode.HeaderSendMode.DEFAULT);
    assertThat(mergedMode.getResponseBodyMode()).isEqualTo(ProcessingMode.BodySendMode.NONE);
  }

  @Test
  public void givenOverrideConfig_whenAllFieldsOverridden_thenAllTakeEffect() throws Exception {
    ExternalProcessor parentProto = createBaseProto(extProcServerName)
        .setFailureModeAllow(false)
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
    ExtProcPerRoute perRoute = ExtProcPerRoute.newBuilder()
        .setOverrides(ExtProcOverrides.newBuilder()
            .setFailureModeAllow(com.google.protobuf.BoolValue.of(true))
            .setGrpcService(overrideService)
            .setProcessingMode(ProcessingMode.newBuilder()
                .setRequestBodyMode(ProcessingMode.BodySendMode.GRPC).build())
            .addRequestAttributes("attr-over")
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

    ExternalProcessorFilterConfig mergedConfig = interceptor.getFilterConfig();
    assertThat(mergedConfig.getFailureModeAllow()).isTrue();
    assertThat(mergedConfig.getExternalProcessor().getGrpcService()).isEqualTo(overrideService);
    assertThat(mergedConfig.getExternalProcessor().getProcessingMode().getRequestBodyMode())
        .isEqualTo(ProcessingMode.BodySendMode.GRPC);
    assertThat(mergedConfig.getExternalProcessor().getRequestAttributesList())
        .containsExactly("attr-over");
  }

  @Test
  public void givenOverrideConfig_whenSomeFieldsOverridden_thenMergedCorrectly() throws Exception {
    ExternalProcessor parentProto = createBaseProto(extProcServerName)
        .setFailureModeAllow(false)
        .addRequestAttributes("attr-parent")
        .build();
    ExtProcPerRoute perRoute = ExtProcPerRoute.newBuilder()
        .setOverrides(ExtProcOverrides.newBuilder()
            .setFailureModeAllow(com.google.protobuf.BoolValue.of(true))
            // requestAttributes NOT set in override
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

    ExternalProcessorFilterConfig mergedConfig = interceptor.getFilterConfig();
    assertThat(mergedConfig.getFailureModeAllow()).isTrue();
    assertThat(mergedConfig.getExternalProcessor().getRequestAttributesList())
        .containsExactly("attr-parent");
  }


  @Test
  public void givenOverrideConfig_whenDisableImmediateResponseOverridden_thenInheritedFromParent()
      throws Exception {
    // disable_immediate_response is NOT in ExtProcOverrides.
    ExternalProcessor parentProto = createBaseProto(extProcServerName)
        .setDisableImmediateResponse(true)
        .build();
    ExtProcPerRoute perRoute = ExtProcPerRoute.newBuilder()
        .setOverrides(ExtProcOverrides.newBuilder().build())
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

    assertThat(interceptor.getFilterConfig().getDisableImmediateResponse()).isTrue();
  }

  @Test
  public void givenOverrideConfig_whenMutationRulesOverridden_thenInheritedFromParent()
      throws Exception {
    // mutation_rules is NOT in ExtProcOverrides.
    io.envoyproxy.envoy.config.common.mutation_rules.v3.HeaderMutationRules rules = 
        io.envoyproxy.envoy.config.common.mutation_rules.v3.HeaderMutationRules.newBuilder()
            .setDisallowAll(com.google.protobuf.BoolValue.newBuilder().setValue(true).build())
            .build();

    ExternalProcessor parentProto = createBaseProto(extProcServerName)
        .setMutationRules(rules)
        .build();
    ExtProcPerRoute perRoute = ExtProcPerRoute.newBuilder()
        .setOverrides(ExtProcOverrides.newBuilder().build())
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

    assertThat(interceptor.getFilterConfig().getMutationRulesConfig().get().disallowAll())
        .isTrue();
  }

  @Test
  public void givenOverrideConfig_whenDeferredCloseTimeoutOverridden_thenInheritedFromParent()
      throws Exception {
    // deferred_close_timeout is NOT in ExtProcOverrides.
    ExternalProcessor parentProto = createBaseProto(extProcServerName)
        .setDeferredCloseTimeout(com.google.protobuf.Duration.newBuilder().setSeconds(10).build())
        .build();
    ExtProcPerRoute perRoute = ExtProcPerRoute.newBuilder()
        .setOverrides(ExtProcOverrides.newBuilder().build())
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

    assertThat(interceptor.getFilterConfig().getDeferredCloseTimeoutNanos())
        .isEqualTo(TimeUnit.SECONDS.toNanos(10));
  }
}
