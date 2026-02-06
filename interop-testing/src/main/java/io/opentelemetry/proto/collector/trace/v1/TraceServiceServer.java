package io.opentelemetry.proto.collector.trace.v1;

import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TraceServiceServer {

  private int port = 7080;
  private Server server;
  private ScheduledExecutorService executor;
  public static void main(String[] args) throws Exception {
    final TraceServiceServer server = new TraceServiceServer();
    server.parseArgs(args);


    Runtime.getRuntime()
        .addShutdownHook(
            new Thread() {
              @Override
              @SuppressWarnings("CatchAndPrintStackTrace")
              public void run() {
                try {
                  System.out.println("Shutting down");
                  server.stop();
                } catch (Exception e) {
                  e.printStackTrace();
                }
              }
            });
    server.start();
    System.out.println("Server started on port " + server.port);
    server.blockUntilShutdown();
  }

  private void start() {
    executor = Executors.newSingleThreadScheduledExecutor();
    server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
        .addService(new TraceServiceImpl()).build();
  }

  void stop() throws Exception {
    server.shutdownNow();
    if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
      System.err.println("Timed out waiting for server shutdown");
    }
    MoreExecutors.shutdownAndAwaitTermination(executor, 5, TimeUnit.SECONDS);
  }

  /** Await termination on the main thread since the grpc library uses daemon threads. */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  private void parseArgs(String[] args) {
    boolean usage = false;
    for (String arg : args) {
      if (!arg.startsWith("--")) {
        System.err.println("All arguments must start with '--': " + arg);
        usage = true;
        break;
      }
      String[] parts = arg.substring(2).split("=", 2);
      String key = parts[0];
      if ("help".equals(key)) {
        usage = true;
        break;
      }
    }
    if (usage) {
      System.out.println(
          "Usage: [ARGS...]"
              + "\n"
              + "\n  --port=PORT           Port to connect to. Default " + port);
      System.exit(1);
    }
  }
}
