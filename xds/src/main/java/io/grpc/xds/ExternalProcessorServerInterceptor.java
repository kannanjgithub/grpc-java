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

import static com.google.common.base.Preconditions.checkNotNull;
import static io.grpc.xds.ExternalProcessorFilter.clientHalfCloseDuration;
import static io.grpc.xds.ExternalProcessorFilter.clientHeadersDuration;
import static io.grpc.xds.ExternalProcessorFilter.outboundStreamToByteString;
import static io.grpc.xds.ExternalProcessorFilter.serverHeadersDuration;
import static io.grpc.xds.ExternalProcessorFilter.serverTrailersDuration;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.envoyproxy.envoy.config.core.v3.HeaderMap;
import io.envoyproxy.envoy.extensions.filters.http.ext_proc.v3.ProcessingMode;
import io.envoyproxy.envoy.service.ext_proc.v3.BodyMutation;
import io.envoyproxy.envoy.service.ext_proc.v3.BodyResponse;
import io.envoyproxy.envoy.service.ext_proc.v3.CommonResponse;
import io.envoyproxy.envoy.service.ext_proc.v3.ExternalProcessorGrpc;
import io.envoyproxy.envoy.service.ext_proc.v3.HeaderMutation;
import io.envoyproxy.envoy.service.ext_proc.v3.HttpBody;
import io.envoyproxy.envoy.service.ext_proc.v3.HttpHeaders;
import io.envoyproxy.envoy.service.ext_proc.v3.HttpTrailers;
import io.envoyproxy.envoy.service.ext_proc.v3.ImmediateResponse;
import io.envoyproxy.envoy.service.ext_proc.v3.ProcessingRequest;
import io.envoyproxy.envoy.service.ext_proc.v3.ProcessingResponse;
import io.envoyproxy.envoy.service.ext_proc.v3.ProtocolConfiguration;
import io.envoyproxy.envoy.service.ext_proc.v3.StreamedBodyResponse;
import io.grpc.Context;
import io.grpc.DoubleHistogramMetricInstrument;
import io.grpc.Drainable;
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.KnownLength;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.MetricRecorder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.SynchronizationContext;
import io.grpc.internal.GrpcUtil;
import io.grpc.internal.SharedResourceHolder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.MetadataUtils;
import io.grpc.xds.ExternalProcessorFilter.DataPlaneCallState;
import io.grpc.xds.ExternalProcessorFilter.ExtProcStreamState;
import io.grpc.xds.ExternalProcessorFilter.ExternalProcessorFilterConfig;
import io.grpc.xds.ExternalProcessorFilter.HeaderForwardingRulesConfig;
import io.grpc.xds.Filter.FilterContext;
import io.grpc.xds.internal.KnownLengthInputStream;
import io.grpc.xds.internal.grpcservice.CachedChannelManager;
import io.grpc.xds.internal.grpcservice.HeaderValue;
import io.grpc.xds.internal.grpcservice.HeaderValueValidationUtils;
import io.grpc.xds.internal.headermutations.HeaderMutationDisallowedException;
import io.grpc.xds.internal.headermutations.HeaderMutationFilter;
import io.grpc.xds.internal.headermutations.HeaderMutationRulesConfig;
import io.grpc.xds.internal.headermutations.HeaderMutations;
import io.grpc.xds.internal.headermutations.HeaderMutator;
import io.grpc.xds.internal.headermutations.HeaderValueOption;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Server-side interceptor for external processing filter.
 */
final class ExternalProcessorServerInterceptor implements ServerInterceptor {
  private static final Logger logger = Logger.getLogger(ExternalProcessorServerInterceptor.class.getName());

  private final ExternalProcessorFilterConfig filterConfig;
  private final MetricRecorder metricsRecorder;
  private final ManagedChannel extProcChannel;

  ExternalProcessorServerInterceptor(
      ExternalProcessorFilterConfig filterConfig,
      CachedChannelManager cachedChannelManager,
      FilterContext context) {
    this.filterConfig = checkNotNull(filterConfig, "filterConfig");
    checkNotNull(cachedChannelManager, "cachedChannelManager");
    this.metricsRecorder = checkNotNull(context.metricsRecorder(), "metricsRecorder");
    ExternalProcessorFilter.initMetricInstruments();
    this.extProcChannel = cachedChannelManager.getChannel(filterConfig.getGrpcServiceConfig());
  }

  ExternalProcessorFilterConfig getFilterConfig() {
    return filterConfig;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    ServerCall<InputStream, InputStream> rawCall =
        (ServerCall<InputStream, InputStream>) (ServerCall<?, ?>) call;
    ServerCallHandler<InputStream, InputStream> rawNext =
        (ServerCallHandler<InputStream, InputStream>) (ServerCallHandler<?, ?>) next;

    ScheduledExecutorService scheduler = SharedResourceHolder.get(GrpcUtil.TIMER_SERVICE);
    ExternalProcessorGrpc.ExternalProcessorStub extProcStub = ExternalProcessorGrpc.newStub(
        extProcChannel)
        .withExecutor(MoreExecutors.directExecutor());

    if (filterConfig.getGrpcServiceConfig().timeout().isPresent()) {
      long timeoutNanos = filterConfig.getGrpcServiceConfig().timeout().get().toNanos();
      if (timeoutNanos > 0) {
        extProcStub = extProcStub.withDeadlineAfter(timeoutNanos, TimeUnit.NANOSECONDS);
      }
    }
    if (filterConfig.getGrpcServiceConfig().initialMetadata() != null
        && !filterConfig.getGrpcServiceConfig().initialMetadata().isEmpty()) {
      Metadata extraHeaders = new Metadata();
      for (HeaderValue headerValue : filterConfig.getGrpcServiceConfig().initialMetadata()) {
        String key = headerValue.key();
        if (key.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
          if (headerValue.rawValue().isPresent()) {
            Metadata.Key<byte[]> metadataKey =
                Metadata.Key.of(key, Metadata.BINARY_BYTE_MARSHALLER);
            extraHeaders.put(metadataKey, headerValue.rawValue().get().toByteArray());
          }
        } else {
          if (headerValue.value().isPresent()) {
            Metadata.Key<String> metadataKey =
                Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER);
            extraHeaders.put(metadataKey, headerValue.value().get());
          }
        }
      }
      extProcStub = extProcStub.withInterceptors(
          MetadataUtils.newAttachHeadersInterceptor(extraHeaders));
    }

    Context callContext = Context.current();

    DataPlaneServerCall dataPlaneServerCall = new DataPlaneServerCall(
        rawCall, extProcStub, filterConfig, filterConfig.getMutationRulesConfig(),
        scheduler, call.getMethodDescriptor(), metricsRecorder, call.getAuthority(), rawNext, headers,
        callContext);

    dataPlaneServerCall.start();

    return (ServerCall.Listener<ReqT>) (ServerCall.Listener<?>) dataPlaneServerCall.getListener();
  }

  private static HeaderMap toHeaderMap(
      Metadata metadata, Optional<HeaderForwardingRulesConfig> forwardRules) {
    HeaderMap.Builder builder = HeaderMap.newBuilder();

    for (String key : metadata.keys()) {
      if (forwardRules.isPresent() && !forwardRules.get().isAllowed(key)) {
        continue;
      }
      if (key.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
        Metadata.Key<byte[]> binKey = Metadata.Key.of(key, Metadata.BINARY_BYTE_MARSHALLER);
        Iterable<byte[]> values = metadata.getAll(binKey);
        if (values != null) {
          for (byte[] binValue : values) {
            String base64Value = BaseEncoding.base64().encode(binValue);
            io.envoyproxy.envoy.config.core.v3.HeaderValue headerValue =
                io.envoyproxy.envoy.config.core.v3.HeaderValue.newBuilder()
                    .setKey(key.toLowerCase(Locale.ROOT))
                    .setRawValue(ByteString.copyFromUtf8(base64Value))
                    .build();
            builder.addHeaders(headerValue);
          }
        }
      } else {
        Metadata.Key<String> asciiKey = Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER);
        Iterable<String> values = metadata.getAll(asciiKey);
        if (values != null) {
          for (String value : values) {
            io.envoyproxy.envoy.config.core.v3.HeaderValue headerValue =
                io.envoyproxy.envoy.config.core.v3.HeaderValue.newBuilder()
                    .setKey(key.toLowerCase(Locale.ROOT))
                    .setRawValue(ByteString.copyFromUtf8(value))
                    .build();
            builder.addHeaders(headerValue);
          }
        }
      }
    }
    return builder.build();
  }

  private static ImmutableMap<String, Struct> collectAttributes(
      ImmutableList<String> requestedAttributes,
      MethodDescriptor<?, ?> method,
      String authority,
      Metadata headers) {
    if (requestedAttributes.isEmpty()) {
      return ImmutableMap.of();
    }
    ImmutableMap.Builder<String, Struct> attributes = ImmutableMap.builder();
    for (String attr : requestedAttributes) {
      switch (attr) {
        case "request.path":
        case "request.url_path":
          attributes.put(attr, encodeAttribute("/" + method.getFullMethodName()));
          break;
        case "request.host":
          attributes.put(attr, encodeAttribute(authority));
          break;
        case "request.method":
          attributes.put(attr, encodeAttribute("POST"));
          break;
        case "request.headers":
          attributes.put(attr, encodeHeaders(headers));
          break;
        case "request.referer":
          String referer = getHeaderValue(headers, "referer");
          if (referer != null) {
            attributes.put(attr, encodeAttribute(referer));
          }
          break;
        case "request.useragent":
          String ua = getHeaderValue(headers, "user-agent");
          if (ua != null) {
            attributes.put(attr, encodeAttribute(ua));
          }
          break;
        case "request.id":
          String id = getHeaderValue(headers, "x-request-id");
          if (id != null) {
            attributes.put(attr, encodeAttribute(id));
          }
          break;
        case "request.query":
          attributes.put(attr, encodeAttribute(""));
          break;
        default:
          break;
      }
    }
    return attributes.buildOrThrow();
  }

  private static Struct encodeAttribute(String value) {
    return Struct.newBuilder()
        .putFields("", Value.newBuilder().setStringValue(value).build())
        .build();
  }

  private static Struct encodeHeaders(Metadata headers) {
    Struct.Builder builder = Struct.newBuilder();
    for (String key : headers.keys()) {
      String value = getHeaderValue(headers, key);
      if (value != null) {
        builder.putFields(key.toLowerCase(Locale.ROOT),
            Value.newBuilder().setStringValue(value).build());
      }
    }
    return builder.build();
  }

  @Nullable
  private static String getHeaderValue(Metadata headers, String headerName) {
    if (headerName.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
      Metadata.Key<byte[]> key = Metadata.Key.of(headerName, Metadata.BINARY_BYTE_MARSHALLER);
      Iterable<byte[]> values = headers.getAll(key);
      if (values == null) {
        return null;
      }
      List<String> encoded = new ArrayList<>();
      for (byte[] v : values) {
        encoded.add(BaseEncoding.base64().omitPadding().encode(v));
      }
      return Joiner.on(",").join(encoded);
    }
    Metadata.Key<String> key = Metadata.Key.of(headerName, Metadata.ASCII_STRING_MARSHALLER);
    Iterable<String> values = headers.getAll(key);
    return values == null ? null : Joiner.on(",").join(values);
  }

  private static class DataPlaneServerCall extends SimpleForwardingServerCall<InputStream, InputStream> {
    private enum EventType {
      REQUEST_HEADERS,
      REQUEST_BODY,
      RESPONSE_HEADERS,
      RESPONSE_BODY,
      RESPONSE_TRAILERS
    }

    private final ServerCall<InputStream, InputStream> rawCall;
    private final ExternalProcessorGrpc.ExternalProcessorStub extProcStub;
    private final SynchronizationContext syncContext;
    private final ExternalProcessorFilterConfig config;
    private final ScheduledExecutorService scheduler;
    private final Object streamLock = new Object();
    private final Object rawCallLock = new Object();
    private final Queue<EventType> expectedResponses = new ConcurrentLinkedQueue<>();
    private volatile ClientCallStreamObserver<ProcessingRequest> extProcClientCallRequestObserver;
    private final Queue<InputStream> pendingDrainingMessages = new ConcurrentLinkedQueue<>();
    private final Queue<InputStream> savedOutgoingMessages = new ConcurrentLinkedQueue<>();
    private volatile DataPlaneServerListener wrappedListener;
    private final HeaderMutationFilter mutationFilter;
    private final HeaderMutator mutator = HeaderMutator.create();
    private final AtomicInteger pendingRequests = new AtomicInteger(0);
    private final ProcessingMode currentProcessingMode;
    private final MethodDescriptor<?, ?> method;
    private final MetricRecorder metricsRecorder;
    private final String authority;
    private final ServerCallHandler<InputStream, InputStream> rawNext;
    private final Context callContext;
    private volatile Metadata requestHeaders;

    private volatile Metadata savedResponseHeaders;
    private volatile Status savedStatus;
    private volatile Metadata savedTrailers;

    private boolean protocolConfigSent = false;
    private ImmutableMap<String, Struct> collectedAttributes;
    private boolean requestAttributesSent = false;

    private long clientHeadersStartNanos;
    private long clientHalfCloseStartNanos;
    private long serverHeadersStartNanos;
    private long serverTrailersStartNanos;

    final AtomicReference<DataPlaneCallState> dataPlaneCallState =
        new AtomicReference<>(DataPlaneCallState.IDLE);
    final AtomicReference<ExtProcStreamState> extProcStreamState =
        new AtomicReference<>(ExtProcStreamState.ACTIVE);
    final AtomicBoolean passThroughMode = new AtomicBoolean(false);
    final AtomicBoolean halfClosed = new AtomicBoolean(false);
    final AtomicBoolean requestSideClosed = new AtomicBoolean(false);
    final AtomicBoolean dataPlaneCallClosed = new AtomicBoolean(false);
    final AtomicBoolean bodyMessageSentToExtProc = new AtomicBoolean(false);

    final AtomicBoolean isProcessingTrailers = new AtomicBoolean(false);
    final AtomicBoolean responseHeadersSent = new AtomicBoolean(false);
    final AtomicBoolean trailersOnly = new AtomicBoolean(false);
    final AtomicBoolean terminationTriggered = new AtomicBoolean(false);

    protected DataPlaneServerCall(
        ServerCall<InputStream, InputStream> rawCall,
        ExternalProcessorGrpc.ExternalProcessorStub extProcStub,
        ExternalProcessorFilterConfig config,
        Optional<HeaderMutationRulesConfig> mutationRulesConfig,
        ScheduledExecutorService scheduler,
        MethodDescriptor<?, ?> method,
        MetricRecorder metricsRecorder,
        String authority,
        ServerCallHandler<InputStream, InputStream> rawNext,
        Metadata requestHeaders,
        Context callContext) {
      super(rawCall);
      this.rawCall = rawCall;
      this.syncContext = new SynchronizationContext(new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          logger.log(
              Level.SEVERE,
              "Uncaught exception in ExternalProcessorServerInterceptor SynchronizationContext",
              e);
        }
      });
      this.extProcStub = extProcStub.withExecutor(this.syncContext);
      this.config = config;
      this.currentProcessingMode = config.getExternalProcessor().getProcessingMode();
      this.mutationFilter = new HeaderMutationFilter(mutationRulesConfig);
      this.scheduler = scheduler;
      this.method = method;
      this.metricsRecorder = checkNotNull(metricsRecorder, "metricsRecorder");
      this.authority = authority;
      this.rawNext = rawNext;
      this.requestHeaders = requestHeaders;
      this.callContext = callContext;
      this.wrappedListener = new DataPlaneServerListener(this);
    }

    ServerCall.Listener<InputStream> getListener() {
      return wrappedListener;
    }

    boolean isExtProcStreamCompleted() {
      ExtProcStreamState s = extProcStreamState.get();
      return s == ExtProcStreamState.COMPLETED || s == ExtProcStreamState.FAILED;
    }

    boolean isExtProcStreamFailed() {
      return extProcStreamState.get() == ExtProcStreamState.FAILED;
    }

    boolean isExtProcStreamDraining() {
      return extProcStreamState.get() == ExtProcStreamState.DRAINING;
    }

    boolean markExtProcStreamCompleted() {
      while (true) {
        ExtProcStreamState current = extProcStreamState.get();
        if (current == ExtProcStreamState.COMPLETED || current == ExtProcStreamState.FAILED) {
          return false;
        }
        if (extProcStreamState.compareAndSet(current, ExtProcStreamState.COMPLETED)) {
          return true;
        }
      }
    }

    boolean markExtProcStreamFailed() {
      while (true) {
        ExtProcStreamState current = extProcStreamState.get();
        if (current == ExtProcStreamState.COMPLETED || current == ExtProcStreamState.FAILED) {
          return false;
        }
        if (extProcStreamState.compareAndSet(current, ExtProcStreamState.FAILED)) {
          return true;
        }
      }
    }

    boolean markDataPlaneCallClosed() {
      while (true) {
        DataPlaneCallState current = dataPlaneCallState.get();
        if (current == DataPlaneCallState.CLOSED) {
          return false;
        }
        if (dataPlaneCallState.compareAndSet(current, DataPlaneCallState.CLOSED)) {
          return true;
        }
      }
    }

    private void activateCall() {
      if ((extProcStreamState.get() == ExtProcStreamState.FAILED
              && !config.getObservabilityMode()
              && (!config.getFailureModeAllow() || bodyMessageSentToExtProc.get()))
          || !dataPlaneCallState.compareAndSet(
              DataPlaneCallState.IDLE, DataPlaneCallState.ACTIVE)) {
        return;
      }
      if (clientHeadersStartNanos > 0) {
        long durationNanos = System.nanoTime() - clientHeadersStartNanos;
        recordDuration(clientHeadersDuration, durationNanos);
        clientHeadersStartNanos = 0;
      }
      Context previous = callContext.attach();
      ServerCall.Listener<InputStream> appListener;
      try {
        appListener = rawNext.startCall(this, requestHeaders);
      } finally {
        callContext.detach(previous);
      }
      wrappedListener.setDelegate(appListener);
      drainPendingRequests();
      wrappedListener.onReadyNotify();
      if (wrappedListener.halfCloseDeferred) {
        wrappedListener.handleDeferredHalfClose();
      }
    }

    private void recordDuration(DoubleHistogramMetricInstrument instrument, long durationNanos) {
      if (instrument != null) {
        double durationSecs = (double) durationNanos / 1_000_000_000.0;
        metricsRecorder.recordDoubleHistogram(
            instrument,
            durationSecs,
            ImmutableList.of("server"),
            ImmutableList.of("server"));
      }
    }

    private boolean validateCompressionSupport(BodyResponse bodyResponse) {
      if (bodyResponse.hasResponse() && bodyResponse.getResponse().hasBodyMutation()) {
        BodyMutation mutation = bodyResponse.getResponse().getBodyMutation();
        if (mutation.hasStreamedResponse()
            && mutation.getStreamedResponse().getGrpcMessageCompressed()) {
          StatusRuntimeException ex = Status.UNAVAILABLE
              .withDescription("gRPC message compression not supported in ext_proc")
              .asRuntimeException();
          synchronized (streamLock) {
            if (!isExtProcStreamCompleted() && extProcClientCallRequestObserver != null) {
              extProcClientCallRequestObserver.onError(ex);
            }
          }
          activateCall();
          markExtProcStreamFailed();
          rawCall.close(Status.UNAVAILABLE.withDescription("gRPC message compression not supported in ext_proc"), new Metadata());
          closeExtProcStream();
          return false;
        }
      }
      return true;
    }

    private void applyHeaderMutations(Metadata metadata, HeaderMutation mutation)
        throws HeaderMutationDisallowedException {
      if (metadata == null) {
        return;
      }
      ImmutableList.Builder<HeaderValueOption> headersToModify = ImmutableList.builder();
      for (io.envoyproxy.envoy.config.core.v3.HeaderValueOption protoOption
          : mutation.getSetHeadersList()) {
        io.envoyproxy.envoy.config.core.v3.HeaderValue protoHeader = protoOption.getHeader();
        String key = protoHeader.getKey();
        HeaderValueValidationUtils.validateHeaderKey(key);

        ByteString rawBytes = protoHeader.getRawValue();
        if (rawBytes.isEmpty()) {
          rawBytes = ByteString.copyFromUtf8(protoHeader.getValue());
        }

        if (rawBytes.size() > HeaderValueValidationUtils.MAX_HEADER_LENGTH) {
          throw new IllegalArgumentException(
              "Header value length exceeds maximum allowed length: " + rawBytes.size());
        }

        HeaderValue headerValue;
        if (key.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
          byte[] decodedBytes = BaseEncoding.base64().decode(rawBytes.toStringUtf8());
          headerValue = HeaderValue.create(key, ByteString.copyFrom(decodedBytes));
        } else {
          headerValue = HeaderValue.create(key, rawBytes.toStringUtf8());
        }
        headersToModify.add(HeaderValueOption.create(
            headerValue,
            HeaderValueOption.HeaderAppendAction.valueOf(protoOption.getAppendAction().name())));
      }

      ImmutableList.Builder<String> headersToRemove = ImmutableList.builder();
      for (String headerToRemove : mutation.getRemoveHeadersList()) {
        HeaderValueValidationUtils.validateHeaderKey(headerToRemove);
        headersToRemove.add(headerToRemove);
      }

      HeaderMutations mutations = HeaderMutations.create(
          headersToModify.build(),
          headersToRemove.build());

      HeaderMutations filteredMutations = mutationFilter.filter(mutations);
      mutator.applyMutations(filteredMutations, metadata);
    }

    void start() {
      clientHeadersStartNanos = System.nanoTime();
      this.collectedAttributes = collectAttributes(
          config.getRequestAttributes(), method, authority, requestHeaders);

      extProcStub.process(new ClientResponseObserver<ProcessingRequest, ProcessingResponse>() {
        @Override
        public void beforeStart(ClientCallStreamObserver<ProcessingRequest> requestStream) {
          synchronized (streamLock) {
            extProcClientCallRequestObserver = requestStream;
          }
          requestStream.setOnReadyHandler(DataPlaneServerCall.this::onExtProcStreamReady);
        }

        @Override
        public void onNext(ProcessingResponse response) {
          try {
            if (config.getObservabilityMode()) {
              return;
            }

            if (response.hasImmediateResponse()) {
              if (config.getDisableImmediateResponse()) {
                internalOnError(Status.UNAVAILABLE
                    .withDescription(
                        "Immediate response is disabled but received from external processor")
                    .asRuntimeException());
                return;
              }
              handleImmediateResponse(response.getImmediateResponse());
              return;
            }

            EventType expected = expectedResponses.peek();
            EventType received = null;
            if (response.hasRequestHeaders()) {
              received = EventType.REQUEST_HEADERS;
            } else if (response.hasRequestBody()) {
              received = EventType.REQUEST_BODY;
            } else if (response.hasResponseHeaders()) {
              received = EventType.RESPONSE_HEADERS;
            } else if (response.hasResponseBody()) {
              received = EventType.RESPONSE_BODY;
            } else if (response.hasResponseTrailers()) {
              received = EventType.RESPONSE_TRAILERS;
            }

            if (received != null) {
              if (expected == null || expected != received) {
                internalOnError(Status.UNAVAILABLE
                    .withDescription("Protocol error: received response out of order. Expected: " 
                        + expected + ", Received: " + received)
                    .asRuntimeException());
                return;
              }
              expectedResponses.poll();
            }

            if (response.getRequestDrain()) {
              extProcStreamState.set(ExtProcStreamState.DRAINING);
              halfCloseExtProcStream();
              activateCall();
            }

            if (response.hasRequestHeaders()) {
              if (response.getRequestHeaders().hasResponse()) {
                if (response.getRequestHeaders().getResponse().getStatus()
                    == CommonResponse.ResponseStatus.CONTINUE_AND_REPLACE) {
                  internalOnError(Status.UNAVAILABLE
                      .withDescription("CONTINUE_AND_REPLACE is not supported")
                      .asRuntimeException());
                  return;
                }
                applyHeaderMutations(
                    requestHeaders,
                    response.getRequestHeaders().getResponse().getHeaderMutation());
              }
              activateCall();
            }
            else if (response.hasRequestBody()) {
              if (validateCompressionSupport(response.getRequestBody())) {
                handleRequestBodyResponse(response.getRequestBody());
              }
            }
            else if (response.hasResponseHeaders()) {
              if (response.getResponseHeaders().hasResponse()) {
                if (response.getResponseHeaders().getResponse().getStatus()
                    == CommonResponse.ResponseStatus.CONTINUE_AND_REPLACE) {
                  internalOnError(Status.UNAVAILABLE
                      .withDescription("CONTINUE_AND_REPLACE is not supported")
                      .asRuntimeException());
                  return;
                }
                applyHeaderMutations(
                    trailersOnly.get() ? savedTrailers : savedResponseHeaders,
                    response.getResponseHeaders().getResponse().getHeaderMutation());
              }
              if (trailersOnly.get()) {
                proceedWithClose();
              } else {
                proceedWithSendHeaders();
              }
            }
            else if (response.hasResponseBody()) {
              if (validateCompressionSupport(response.getResponseBody())) {
                handleResponseBodyResponse(response.getResponseBody());
              }
            }
            else if (response.hasResponseTrailers()) {
              if (response.getResponseTrailers().hasHeaderMutation()) {
                applyHeaderMutations(
                    savedTrailers,
                    response.getResponseTrailers().getHeaderMutation()
                );
              }
              proceedWithClose();
            }

            checkEndOfStream();
          } catch (Throwable t) {
            internalOnError(t);
          }
        }

        @Override
        public void onError(Throwable t) {
          if (markExtProcStreamFailed()) {
            synchronized (streamLock) {
              extProcClientCallRequestObserver = null;
            }
            if (config.getObservabilityMode()
                || (config.getFailureModeAllow() && !bodyMessageSentToExtProc.get())) {
              handleFailOpen();
            } else {
              rawCall.close(Status.INTERNAL.withDescription("External processor stream failed").withCause(t), new Metadata());
            }
          }
        }

        @Override
        public void onCompleted() {
          if (markExtProcStreamCompleted()) {
            handleFailOpen();
          }
        }
      });

      boolean sendRequestHeaders =
          currentProcessingMode.getRequestHeaderMode() == ProcessingMode.HeaderSendMode.SEND
          || currentProcessingMode.getRequestHeaderMode()
              == ProcessingMode.HeaderSendMode.DEFAULT;

      if (sendRequestHeaders) {
        sendToExtProc(ProcessingRequest.newBuilder()
            .setRequestHeaders(HttpHeaders.newBuilder()
                .setHeaders(toHeaderMap(requestHeaders, config.getForwardRulesConfig()))
                .setEndOfStream(false)
                .build())
            .build());
      }

      if (config.getObservabilityMode() || !sendRequestHeaders) {
        activateCall();
      }
    }

    private void sendToExtProc(ProcessingRequest request) {
      synchronized (streamLock) {
        if (isExtProcStreamCompleted()) {
          return;
        }

        ProcessingRequest requestToSend = request;
        if (!protocolConfigSent) {
          requestToSend = ProcessingRequest.newBuilder(requestToSend)
              .setProtocolConfig(ProtocolConfiguration.newBuilder()
                  .setRequestBodyMode(currentProcessingMode.getRequestBodyMode())
                  .setResponseBodyMode(currentProcessingMode.getResponseBodyMode())
                  .build())
              .build();
          protocolConfigSent = true;
        }

        boolean isClientServerMessage =
            requestToSend.hasRequestHeaders() || requestToSend.hasRequestBody();
        if (isClientServerMessage
            && !requestAttributesSent
            && collectedAttributes != null
            && !collectedAttributes.isEmpty()) {
          requestToSend = ProcessingRequest.newBuilder(requestToSend)
              .putAllAttributes(collectedAttributes)
              .build();
          requestAttributesSent = true;
        }

        if (config.getObservabilityMode()) {
          requestToSend = ProcessingRequest.newBuilder(requestToSend)
              .setObservabilityMode(true)
              .build();
        }
        
        if (requestToSend.hasRequestHeaders()) {
          expectedResponses.add(EventType.REQUEST_HEADERS);
        } else if (requestToSend.hasRequestBody()) {
          expectedResponses.add(EventType.REQUEST_BODY);
        } else if (requestToSend.hasResponseHeaders()) {
          expectedResponses.add(EventType.RESPONSE_HEADERS);
        } else if (requestToSend.hasResponseBody()) {
          expectedResponses.add(EventType.RESPONSE_BODY);
        } else if (requestToSend.hasResponseTrailers()) {
          expectedResponses.add(EventType.RESPONSE_TRAILERS);
        }

        extProcClientCallRequestObserver.onNext(requestToSend);
      }
    }

    private void onExtProcStreamReady() {
      drainPendingRequests();
      wrappedListener.onReadyNotify();
    }

    private void drainPendingRequests() {
      int toRequest = pendingRequests.getAndSet(0);
      if (toRequest > 0) {
        super.request(toRequest);
      }
    }

    private void closeExtProcStream() {
      synchronized (streamLock) {
        if (markExtProcStreamCompleted()) {
          if (extProcClientCallRequestObserver != null) {
            extProcClientCallRequestObserver.onCompleted();
          }
        }
        expectedResponses.clear();
      }
      proceedWithClose();
    }

    private void cancelExtProcStream(Throwable t) {
      if (markExtProcStreamFailed()) {
        synchronized (streamLock) {
          if (extProcClientCallRequestObserver != null) {
            try {
              extProcClientCallRequestObserver.onError(t);
            } catch (Throwable ignored) {
            }
            extProcClientCallRequestObserver = null;
          }
        }
        expectedResponses.clear();
        proceedWithClose();
      }
    }

    private void internalOnError(Throwable t) {
      if (markExtProcStreamFailed()) {
        synchronized (streamLock) {
          if (extProcClientCallRequestObserver != null) {
            try {
              extProcClientCallRequestObserver.onError(t);
            } catch (Throwable ignored) {
            }
            extProcClientCallRequestObserver = null;
          }
        }
        expectedResponses.clear();
        if (config.getObservabilityMode()
            || (config.getFailureModeAllow() && !bodyMessageSentToExtProc.get())) {
          handleFailOpen();
        } else {
          rawCall.close(Status.INTERNAL.withDescription("External processor stream failed").withCause(t), new Metadata());
        }
      }
    }

    private void halfCloseExtProcStream() {
      synchronized (streamLock) {
        if (!isExtProcStreamCompleted() && extProcClientCallRequestObserver != null) {
          extProcClientCallRequestObserver.onCompleted();
        }
      }
    }

    private boolean isSidecarReady() {
      if (isExtProcStreamCompleted()) {
        return true;
      }
      if (isExtProcStreamDraining()) {
        return false;
      }
      synchronized (streamLock) {
        ClientCallStreamObserver<ProcessingRequest> observer = extProcClientCallRequestObserver;
        return observer != null && observer.isReady();
      }
    }

    @Override
    public boolean isReady() {
      if (passThroughMode.get()) {
        return super.isReady();
      }
      if (isExtProcStreamCompleted()) {
        return super.isReady();
      }
      if (dataPlaneCallState.get() == DataPlaneCallState.IDLE && !config.getObservabilityMode()) {
        return false;
      }
      boolean sidecarReady = isSidecarReady();
      if (config.getObservabilityMode()) {
        return super.isReady() && sidecarReady;
      }
      return sidecarReady;
    }

    @Override
    public void request(int numMessages) {
      if (passThroughMode.get() || isExtProcStreamCompleted()) {
        super.request(numMessages);
        return;
      }
      if (!isSidecarReady()) {
        pendingRequests.addAndGet(numMessages);
        return;
      }
      super.request(numMessages);
    }

    @Override
    public void sendHeaders(Metadata headers) {

      serverHeadersStartNanos = System.nanoTime();
      responseHeadersSent.set(true);
      boolean sendResponseHeaders =
          currentProcessingMode.getResponseHeaderMode() == ProcessingMode.HeaderSendMode.SEND
          || currentProcessingMode.getResponseHeaderMode()
              == ProcessingMode.HeaderSendMode.DEFAULT;

      synchronized (rawCallLock) {
        // NOTE: Even if sendResponseHeaders is false, we MUST obtain rawCallLock to call
        // proceedWithSendHeaders() safely, because an active control plane thread could
        // concurrently call super.sendMessage() or super.close() (e.g., due to a concurrent error).
        if (passThroughMode.get() || isExtProcStreamCompleted() || !sendResponseHeaders) {
          proceedWithSendHeaders(headers);
          return;
        }
        this.savedResponseHeaders = headers;
        if (isExtProcStreamDraining()) {
          return;
        }
      }

      sendToExtProc(ProcessingRequest.newBuilder()
          .setResponseHeaders(HttpHeaders.newBuilder()
              .setHeaders(toHeaderMap(headers, config.getForwardRulesConfig()))
              .build())
          .build());

      if (config.getObservabilityMode()) {
        synchronized (rawCallLock) {
          proceedWithSendHeaders();
        }
      }
    }

    void proceedWithSendHeaders() {
      synchronized (rawCallLock) {
        if (savedResponseHeaders != null) {
          proceedWithSendHeaders(savedResponseHeaders);
          savedResponseHeaders = null;
          InputStream msg;
          while ((msg = savedOutgoingMessages.poll()) != null) {
            sendMessage(msg);
          }
          if (savedStatus != null) {
            triggerCloseHandshake();
          }
        }
      }
    }

    private void proceedWithSendHeaders(Metadata headers) {
      if (serverHeadersStartNanos > 0) {
        long durationNanos = System.nanoTime() - serverHeadersStartNanos;
        recordDuration(serverHeadersDuration, durationNanos);
        serverHeadersStartNanos = 0;
      }
      super.sendHeaders(headers);
    }

    @Override
    public void sendMessage(InputStream message) {
      if (dataPlaneCallClosed.get()) {
        return;
      }

      // Acquire rawCallLock to safely inspect passThroughMode and state
      synchronized (rawCallLock) {
        if (passThroughMode.get()) {
          super.sendMessage(message);
          return;
        }

        // NOTE: Both checks below must reside inside the synchronized(rawCallLock) block to
        // prevent a Check-Then-Act race condition. If they were checked lock-free, a context
        // switch immediately after the check but before adding to the queue would allow a
        // concurrent control plane thread to finish draining first. The resuming thread would
        // then insert the message into a queue that will never be drained again, causing a hung call.

        // Check-Then-Act: Atomically verify headers sending state and queue message
        if (savedResponseHeaders != null) {
          try {
            ByteString copiedBytes = ByteString.readFrom(message);
            savedOutgoingMessages.add(new KnownLengthInputStream(copiedBytes));
          } catch (IOException e) {
            rawCall.close(Status.INTERNAL.withDescription("Failed to serialize response body").withCause(e), new Metadata());
          }
          return;
        }

        // Check-Then-Act: Atomically verify stream draining state and queue message
        if (isExtProcStreamDraining() || isExtProcStreamCompleted()) {
          try {
            ByteString copiedBytes = ByteString.readFrom(message);
            pendingDrainingMessages.add(new KnownLengthInputStream(copiedBytes));
          } catch (IOException e) {
            rawCall.close(Status.INTERNAL.withDescription("Failed to serialize response body").withCause(e), new Metadata());
          }
          return;
        }
      }

      if (currentProcessingMode.getResponseBodyMode() == ProcessingMode.BodySendMode.NONE) {
        synchronized (rawCallLock) {
          super.sendMessage(message);
        }
        return;
      }

      try {
        ByteString bodyByteString = outboundStreamToByteString(message);
        sendResponseBodyToExtProc(bodyByteString, false);

        if (config.getObservabilityMode()) {
          synchronized (rawCallLock) {
            super.sendMessage(new KnownLengthInputStream(bodyByteString));
          }
        }
      } catch (IOException e) {
        rawCall.close(Status.INTERNAL.withDescription("Failed to serialize response body").withCause(e), new Metadata());
      }
    }

    @Override
    public void close(Status status, Metadata trailers) {
      serverTrailersStartNanos = System.nanoTime();
      if (isExtProcStreamFailed()
          && !config.getObservabilityMode()
          && (!config.getFailureModeAllow() || bodyMessageSentToExtProc.get())) {
        if (markDataPlaneCallClosed()) {
          proceedWithClose(Status.INTERNAL.withDescription("External processor stream failed").withCause(status.getCause()), new Metadata());
        }
        return;
      }

      synchronized (rawCallLock) {
        if (passThroughMode.get()) {
          if (markDataPlaneCallClosed()) {
            proceedWithClose(status, trailers);
          }
          closeExtProcStream();
          return;
        }

        this.savedStatus = status;
        this.savedTrailers = trailers;

        if (isExtProcStreamCompleted()) {
          proceedWithClose();
          return;
        }

        if (savedResponseHeaders != null) {
          return;
        }
      }

      if (!responseHeadersSent.get()) {
        trailersOnly.set(true);
      }

      triggerCloseHandshake();

      if (config.getObservabilityMode()) {
        synchronized (rawCallLock) {
          proceedWithClose();
        }
        @SuppressWarnings("unused")
        ScheduledFuture<?> unused = scheduler.schedule(
            this::closeExtProcStream,
            config.getDeferredCloseTimeoutNanos(),
            TimeUnit.NANOSECONDS);
      }
    }

    void proceedWithClose() {
      synchronized (rawCallLock) {
        if (savedStatus != null 
            && (isExtProcStreamCompleted() || config.getObservabilityMode())) {
          if (markDataPlaneCallClosed()) {
            proceedWithClose(savedStatus, savedTrailers);
          }
          savedStatus = null;
          savedTrailers = null;
        }
      }
    }

    private void proceedWithClose(Status status, Metadata trailers) {
      if (dataPlaneCallClosed.compareAndSet(false, true)) {

        if (serverTrailersStartNanos > 0) {
          long durationNanos = System.nanoTime() - serverTrailersStartNanos;
          recordDuration(serverTrailersDuration, durationNanos);
          serverTrailersStartNanos = 0;
        }
        super.close(status, trailers);
      }
    }

    private void triggerCloseHandshake() {
      if (isExtProcStreamDraining()) {
        return;
      }
      if (isExtProcStreamCompleted() || !terminationTriggered.compareAndSet(false, true)) {
        return;
      }

      boolean sendResponseHeaders =
          currentProcessingMode.getResponseHeaderMode() == ProcessingMode.HeaderSendMode.SEND
          || currentProcessingMode.getResponseHeaderMode()
              == ProcessingMode.HeaderSendMode.DEFAULT;


      boolean sendResponseTrailers =
          currentProcessingMode.getResponseTrailerMode() == ProcessingMode.HeaderSendMode.SEND;

      if (trailersOnly.get()) {
        if (sendResponseHeaders) {
          sendToExtProc(ProcessingRequest.newBuilder()
              .setResponseHeaders(HttpHeaders.newBuilder()
                  .setHeaders(toHeaderMap(savedTrailers, config.getForwardRulesConfig()))
                  .setEndOfStream(true)
                  .build())
              .build());
        } else {
          proceedWithClose();
          if (!config.getObservabilityMode()) {
            closeExtProcStream();
          }
        }
      } else if (sendResponseTrailers) {
        isProcessingTrailers.set(true);
        sendToExtProc(ProcessingRequest.newBuilder()
            .setResponseTrailers(HttpTrailers.newBuilder()
                .setTrailers(toHeaderMap(savedTrailers, config.getForwardRulesConfig()))
                .build())
            .build());
      } else {
        if (isRequestSideCompleted()) {
          unblockAfterStreamComplete();
          closeExtProcStream();
        }
      }
    }

    private void sendResponseBodyToExtProc(@Nullable ByteString bodyByteString, boolean endOfStream) {
      if (isExtProcStreamCompleted()
          || currentProcessingMode.getResponseBodyMode() != ProcessingMode.BodySendMode.GRPC) {
        return;
      }

      HttpBody.Builder bodyBuilder = HttpBody.newBuilder();
      if (bodyByteString != null) {
        bodyBuilder.setBody(bodyByteString);
        bodyMessageSentToExtProc.set(true);
      }
      bodyBuilder.setEndOfStream(endOfStream);

      sendToExtProc(ProcessingRequest.newBuilder()
          .setResponseBody(bodyBuilder.build())
          .build());
    }

    private void handleRequestBodyResponse(BodyResponse bodyResponse) {
      if (bodyResponse.hasResponse()
      && bodyResponse.getResponse().hasBodyMutation()) {
        BodyMutation mutation = bodyResponse.getResponse().getBodyMutation();
        if (mutation.hasStreamedResponse()) {
          StreamedBodyResponse streamed = mutation.getStreamedResponse();
          if (!streamed.getEndOfStreamWithoutMessage()) {
            wrappedListener.onExternalBody(streamed.getBody());
          }
          if (streamed.getEndOfStream() || streamed.getEndOfStreamWithoutMessage()) {
            wrappedListener.proceedWithHalfClose();
          }
        }
      }
    }

    private void handleResponseBodyResponse(BodyResponse bodyResponse) {
      if (dataPlaneCallClosed.get()) {
        return;
      }
      if (bodyResponse.hasResponse() && bodyResponse.getResponse().hasBodyMutation()) {
        BodyMutation mutation = bodyResponse.getResponse().getBodyMutation();
        if (mutation.hasStreamedResponse()) {
          StreamedBodyResponse streamed = mutation.getStreamedResponse();
          if (!streamed.getEndOfStreamWithoutMessage()) {
            super.sendMessage(new KnownLengthInputStream(streamed.getBody()));
          }
        }
      }
    }

    private void handleImmediateResponse(ImmediateResponse immediate)
        throws HeaderMutationDisallowedException {
      Status status = Status.fromCodeValue(immediate.getGrpcStatus().getStatus());
      if (!immediate.getDetails().isEmpty()) {
        status = status.withDescription(immediate.getDetails());
      }

      Metadata trailers = new Metadata();
      if (immediate.hasHeaders()) {
        applyHeaderMutations(trailers, immediate.getHeaders());
      }

      savedStatus = status;
      savedTrailers = trailers;

      if (isProcessingTrailers.get()) {
        unblockAfterStreamComplete();
      } else {
        proceedWithClose(status, trailers);
        unblockAfterStreamComplete();
      }
      closeExtProcStream();
    }

    private void drainPendingDrainingMessages() {
      synchronized (rawCallLock) {
        InputStream msg;
        while ((msg = pendingDrainingMessages.poll()) != null) {
          super.sendMessage(msg);
        }
        passThroughMode.set(true);
      }
    }

    private void handleFailOpen() {
      activateCall();
      drainPendingRequests();
      proceedWithSendHeaders();
      drainPendingDrainingMessages();
      unblockAfterStreamComplete();
      closeExtProcStream();
    }

    /**
     * Evaluates whether the external processor stream can be safely closed and the
     * data plane call terminated.
     *
     * <p>This method acts as a cleanup checkpoint. It is invoked when request-side
     * processing completes (e.g., half-close) or when call termination is triggered.
     *
     * <p>The stream is only closed if:
     * <ul>
     *   <li>Call termination has been initiated ({@code terminationTriggered} is true).</li>
     *   <li>The request side of the call is fully completed ({@code isRequestSideCompleted} is true).</li>
     *   <li>There are no outstanding response-side messages (such as mutated response headers
     *       or trailers) expected from the external processor.</li>
     * </ul>
     *
     * <p>If all conditions are met, the data plane call is unblocked to allow the close status
     * and trailers to be propagated, and the external processor gRPC stream is terminated.
     */
    private void checkEndOfStream() {
      if (terminationTriggered.get() && isRequestSideCompleted()
          && !expectedResponses.contains(EventType.RESPONSE_HEADERS)
          && !expectedResponses.contains(EventType.RESPONSE_TRAILERS)) {
        unblockAfterStreamComplete();
        closeExtProcStream();
      }
    }

    private boolean isRequestSideCompleted() {
      return (currentProcessingMode.getRequestHeaderMode() != ProcessingMode.HeaderSendMode.SEND
          && currentProcessingMode.getRequestBodyMode() != ProcessingMode.BodySendMode.GRPC)
          || requestSideClosed.get();
    }

    void unblockAfterStreamComplete() {
      proceedWithSendHeaders();
      drainPendingDrainingMessages();
      wrappedListener.drainSavedMessages();
      wrappedListener.onReadyNotify();
      proceedWithClose();
    }
  }

  private static final class DataPlaneServerListener extends ServerCall.Listener<InputStream> {
    private final DataPlaneServerCall dataPlaneServerCall;
    private final Queue<InputStream> savedMessages = new ConcurrentLinkedQueue<>();
    private volatile boolean halfCloseReceived;
    private volatile boolean halfCloseDeferred;
    private volatile ServerCall.Listener<InputStream> delegate;

    private DataPlaneServerListener(DataPlaneServerCall dataPlaneServerCall) {
      this.dataPlaneServerCall = dataPlaneServerCall;
    }

    void setDelegate(ServerCall.Listener<InputStream> delegate) {
      dataPlaneServerCall.syncContext.execute(() -> {
        this.delegate = delegate;
        dataPlaneServerCall.callContext.run(() -> {
          InputStream msg;
          while ((msg = savedMessages.poll()) != null) {
            delegate.onMessage(msg);
          }
          if (halfCloseReceived) {
            proceedWithHalfClose();
          }
        });
      });
    }

    void drainSavedMessages() {
      dataPlaneServerCall.syncContext.execute(() -> {
        ServerCall.Listener<InputStream> del = delegate;
        if (del != null) {
          dataPlaneServerCall.callContext.run(() -> {
            InputStream msg;
            while ((msg = savedMessages.poll()) != null) {
              del.onMessage(msg);
            }
            if (halfCloseReceived) {
              proceedWithHalfClose();
            }
          });
        }
      });
    }

    @Override
    public void onReady() {
      dataPlaneServerCall.syncContext.execute(() -> {
        dataPlaneServerCall.drainPendingRequests();
        onReadyNotify();
      });
    }

    void onReadyNotify() {
      ServerCall.Listener<InputStream> del = delegate;
      if (del != null) {
        dataPlaneServerCall.callContext.run(del::onReady);
      }
    }

    @Override
    public void onMessage(InputStream message) {
      dataPlaneServerCall.syncContext.execute(() -> {
        if (dataPlaneServerCall.requestSideClosed.get()) {
          return;
        }
        ServerCall.Listener<InputStream> del = delegate;
        if (dataPlaneServerCall.passThroughMode.get() && del != null) {
          dataPlaneServerCall.callContext.run(() -> del.onMessage(message));
          return;
        }

        // If control stream is finished, or request body processing is disabled,
        // or observability mode is enabled (which ignores mutations)
        // OR the stream is in DRAINING state:
        if (dataPlaneServerCall.isExtProcStreamCompleted()
            || dataPlaneServerCall.isExtProcStreamDraining()
            || dataPlaneServerCall.currentProcessingMode.getRequestBodyMode()
                != ProcessingMode.BodySendMode.GRPC
            || dataPlaneServerCall.config.getObservabilityMode()) {

          if (del == null || dataPlaneServerCall.isExtProcStreamDraining()) {
            // Synchronously copy to the heap to prevent deframer buffer recycling
            try {
              ByteString copiedBytes = ByteString.readFrom(message);
              savedMessages.add(new KnownLengthInputStream(copiedBytes));
            } catch (IOException e) {
              dataPlaneServerCall.rawCall.close(
                  Status.INTERNAL.withDescription("Failed to buffer client request").withCause(e),
                  new Metadata());
            }
          } else {
            dataPlaneServerCall.callContext.run(() -> del.onMessage(message));
          }
          return;
        }

        // Mode is GRPC and not in observability mode: dispatch immediately to ext_proc!
        try {
          ByteString bodyByteString = ByteString.readFrom(message);
          sendRequestBodyToExtProc(bodyByteString, false);
        } catch (IOException e) {
          dataPlaneServerCall.rawCall.close(
              Status.INTERNAL.withDescription("Failed to read client request").withCause(e),
              new Metadata());
        }
      });
    }

    @Override
    public void onHalfClose() {
      dataPlaneServerCall.syncContext.execute(() -> {
        if (dataPlaneServerCall.requestSideClosed.get()) {
          return;
        }
        dataPlaneServerCall.clientHalfCloseStartNanos = System.nanoTime();
        dataPlaneServerCall.halfClosed.set(true);
        halfCloseReceived = true;
        if (dataPlaneServerCall.isExtProcStreamDraining()) {
          return;
        }
        ServerCall.Listener<InputStream> del = delegate;
        if ((dataPlaneServerCall.passThroughMode.get() || dataPlaneServerCall.isExtProcStreamCompleted()) && del != null) {
          proceedWithHalfClose();
          return;
        }

        if (dataPlaneServerCall.dataPlaneCallState.get() == DataPlaneCallState.IDLE) {
          halfCloseDeferred = true;
          return;
        }

        if (dataPlaneServerCall.currentProcessingMode.getRequestBodyMode() == ProcessingMode.BodySendMode.NONE) {
          proceedWithHalfClose();
          return;
        }

        sendRequestBodyToExtProc(null, true);
      });
    }

    void handleDeferredHalfClose() {
      dataPlaneServerCall.syncContext.execute(() -> {
        if (dataPlaneServerCall.currentProcessingMode.getRequestBodyMode() == ProcessingMode.BodySendMode.NONE
            || dataPlaneServerCall.isExtProcStreamCompleted()) {
          proceedWithHalfClose();
        } else {
          sendRequestBodyToExtProc(null, true);
        }
      });
    }

    void proceedWithHalfClose() {
      if (!dataPlaneServerCall.requestSideClosed.compareAndSet(false, true)) {
        return;
      }
      halfCloseReceived = true;
      if (dataPlaneServerCall.clientHalfCloseStartNanos > 0) {
        long durationNanos = System.nanoTime() - dataPlaneServerCall.clientHalfCloseStartNanos;
        dataPlaneServerCall.recordDuration(clientHalfCloseDuration, durationNanos);
        dataPlaneServerCall.clientHalfCloseStartNanos = 0;
      }
      ServerCall.Listener<InputStream> del = delegate;
      if (del != null) {
        dataPlaneServerCall.callContext.run(del::onHalfClose);
      }
      dataPlaneServerCall.checkEndOfStream();
    }

    void onExternalBody(ByteString body) {
      ServerCall.Listener<InputStream> del = delegate;
      // In the future, if zero-copy reads are needed downstream, this can be optimized
      // by wrapping the ByteString in an InputStream that implements HasByteBuffer, KnownLength, and Detachable.
      if (del != null) {
        dataPlaneServerCall.callContext.run(() -> del.onMessage(body.newInput()));
      } else {
        savedMessages.add(body.newInput());
      }
    }

    private void sendRequestBodyToExtProc(@Nullable ByteString bodyByteString, boolean endOfStream) {
      if (dataPlaneServerCall.isExtProcStreamCompleted()
          || dataPlaneServerCall.currentProcessingMode.getRequestBodyMode()
              != ProcessingMode.BodySendMode.GRPC) {
        return;
      }

      HttpBody.Builder bodyBuilder = HttpBody.newBuilder();
      if (bodyByteString != null) {
        bodyBuilder.setBody(bodyByteString);
        bodyBuilder.setEndOfStream(endOfStream);
        dataPlaneServerCall.bodyMessageSentToExtProc.set(true);
      } else {
        bodyBuilder.setEndOfStreamWithoutMessage(true);
      }

      dataPlaneServerCall.sendToExtProc(ProcessingRequest.newBuilder()
          .setRequestBody(bodyBuilder.build())
          .build());
    }

    @Override
    public void onCancel() {
      dataPlaneServerCall.syncContext.execute(() -> {
        dataPlaneServerCall.cancelExtProcStream(
            Status.CANCELLED.withDescription("Client cancelled RPC").asRuntimeException());
        ServerCall.Listener<InputStream> del = delegate;
        if (del != null) {
          dataPlaneServerCall.callContext.run(del::onCancel);
        }
      });
    }

    @Override
    public void onComplete() {
      dataPlaneServerCall.syncContext.execute(() -> {
        ServerCall.Listener<InputStream> del = delegate;
        if (del != null) {
          dataPlaneServerCall.callContext.run(del::onComplete);
        }
      });
    }
  }
}

