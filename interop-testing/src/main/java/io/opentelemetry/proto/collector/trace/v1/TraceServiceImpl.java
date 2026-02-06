package io.opentelemetry.proto.collector.trace.v1;

import com.google.protobuf.ByteString;
import io.grpc.ServerServiceDefinition;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc.AsyncService;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;

import java.util.List;
import java.util.logging.Logger;

public class TraceServiceImpl implements io.grpc.BindableService, AsyncService {
  private final Logger logger = Logger.getLogger(getClass().getName());
  @Override
  public ServerServiceDefinition bindService() {
      return TraceServiceGrpc.bindService(this);
  }

  private String bytesToHex(ByteString bytes) {
    // OpenTelemetry trace IDs are 16 bytes (32 hex characters)
    final byte[] byteArray = bytes.toByteArray();
    final char[] hexChars = new char[byteArray.length * 2];
    for (int j = 0; j < byteArray.length; j++) {
      int v = byteArray[j] & 0xFF;
      // Convert to lowercase hex as per W3C Trace Context spec
      hexChars[j * 2] = Character.forDigit((v >>> 4) & 0x0F, 16);
      hexChars[j * 2 + 1] = Character.forDigit(v & 0x0F, 16);
    }
    return new String(hexChars);
  }

  @Override
  public void export(io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest request,
              io.grpc.stub.StreamObserver<io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse> responseObserver) {
    List<ResourceSpans> resourceSpansList = request.getResourceSpansList();

    // Iterate over all resource spans in the request
    for (ResourceSpans resourceSpans : request.getResourceSpansList()) {
      List<KeyValue> resourceAttrs = resourceSpans.getResource().getAttributesList();
      for (KeyValue resourceAttr: resourceAttrs) {
        logger.info("Resource attr key: " + resourceAttr.getKey() + ", value: " + resourceAttr.getValue());
      }
      // Iterate over all instrumentation scope spans
      for (ScopeSpans scopeSpans : resourceSpans.getScopeSpansList()) {
        // Iterate over individual spans
        for (Span span : scopeSpans.getSpansList()) {
          // Get the trace ID as a ByteString
          ByteString traceIdBytes = span.getTraceId();

          // Convert the byte array to a hex string
          String traceIdHex = bytesToHex(traceIdBytes);

          logger.info("Extracted Trace ID: " + traceIdHex);
          logger.info("Span ID: " + bytesToHex(span.getSpanId()));
        }
      }
    }

    // Create and send the response
    ExportTraceServiceResponse response = ExportTraceServiceResponse.newBuilder().build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
