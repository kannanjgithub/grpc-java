package io.grpc.examples.deadline;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.grpc.Contexts.statusFromCancelled;
import static io.grpc.Status.DEADLINE_EXCEEDED;

class DeadlineServerInterceptor implements ServerInterceptor {
  public static final Metadata.Key<String> GRPC_DEADLINE = Metadata.Key.of("grpc-deadline",
          Metadata.ASCII_STRING_MARSHALLER);

  private final ScheduledExecutorService scheduler;

  public DeadlineServerInterceptor(ScheduledExecutorService scheduler) {
    this.scheduler = scheduler;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                                                               ServerCallHandler<ReqT, RespT> next) {

    Context.CancellableContext context = Context.current()
            .fork()
            .withDeadlineAfter(100, TimeUnit.MILLISECONDS, scheduler);

    context.addListener(ctx -> {
      Status status = statusFromCancelled(ctx);
      if (DEADLINE_EXCEEDED.getCode().equals(status.getCode())) {
        call.close(status, new Metadata());
      }
    }, directExecutor());

    return Contexts.interceptCall(context, call, headers, next);
  }
}