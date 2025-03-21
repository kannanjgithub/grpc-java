package io.grpc.examples.deadline;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.ServerStreamTracer;
import io.grpc.internal.GrpcUtil;

class AbsoluteDeadlineServerStreamTracerFactory extends ServerStreamTracer.Factory {
  public static final Metadata.Key<String> GRPC_DEADLINE = Metadata.Key.of("grpc-deadline",
          Metadata.ASCII_STRING_MARSHALLER);
  public static final ServerStreamTracer NOOP_STREAM_TRACER = new ServerStreamTracer() {
  };
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  @Override
  public ServerStreamTracer newServerStreamTracer(String fullMethodName, Metadata headers) {

    headers.discardAll(GrpcUtil.TIMEOUT_KEY);

    return new ServerStreamTracer() {
      @Override
      public Context filterContext(Context context) {
        return context.withDeadlineAfter(300, TimeUnit.MILLISECONDS, scheduler);
      }
    };
  }
}
