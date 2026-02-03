package io.opentelemetry.proto.collector.trace.v1;

import io.grpc.ServerServiceDefinition;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc.AsyncService;
import io.opentelemetry.proto.trace.v1.ResourceSpans;

import java.util.List;
import java.util.logging.Logger;

public class TraceServiceImpl implements io.grpc.BindableService, AsyncService {
  private final Logger logger = Logger.getLogger(getClass().getName());
  @Override
  public ServerServiceDefinition bindService() {
      return TraceServiceGrpc.bindService(this);
  }

  @Override
  public void export(io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest request,
              io.grpc.stub.StreamObserver<io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse> responseObserver) {
    List<ResourceSpans> resourceSpansList = request.getResourceSpansList();
    logger.info("Received ExportTraceServiceRequest with " + resourceSpansList.size() + " ResourceSpans");

    for (ResourceSpans resourceSpans : resourceSpansList) {
      // Process the spans here (e.g., log them, store them, forward them)
      int spanCount = resourceSpans.getScopeSpansList().stream()
          .mapToInt(ss -> ss.getSpansList().size())
          .sum();
      logger.info(String.format("Resource has %d spans. Resource attributes: %s",
          spanCount, resourceSpans.getResource().getAttributesList()));
    }

    // Create and send the response
    ExportTraceServiceResponse response = ExportTraceServiceResponse.newBuilder().build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
