/*
 * Copyright 2026 The gRPC Authors
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

package io.grpc.xds.internal.extproc;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.grpc.Drainable;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Utility methods for external processing.
 */
public final class ExternalProcessorUtil {
  private ExternalProcessorUtil() {}

  /**
   * Reads an InputStream into a ByteString.
   */
  public static ByteString outboundStreamToByteString(InputStream message) throws IOException {
    if (message instanceof Drainable) {
      int size = message.available();
      ByteString.Output output =
          size > 0 ? ByteString.newOutput(size) : ByteString.newOutput();
      ((Drainable) message).drainTo(output);
      return output.toByteString();
    }
    return ByteString.readFrom(message);
  }

  /**
   * Collects request attributes and structures them as Struct map.
   */
  public static ImmutableMap<String, Struct> collectAttributes(
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
          // "Not set" attributes or unrecognized ones (already validated) are skipped.
          break;
      }
    }
    return attributes.buildOrThrow();
  }

  /**
   * Encodes a single string attribute as Struct.
   */
  public static Struct encodeAttribute(String value) {
    return Struct.newBuilder()
        .putFields("", Value.newBuilder().setStringValue(value).build())
        .build();
  }

  /**
   * Encodes metadata headers as Struct.
   */
  public static Struct encodeHeaders(Metadata headers) {
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

  /**
   * Gets a header value from metadata.
   */
  @Nullable
  public static String getHeaderValue(Metadata headers, String headerName) {
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

  /**
   * Transitions ExtProcStreamState to COMPLETED.
   */
  public static boolean markExtProcStreamCompleted(
      AtomicReference<ExtProcStreamState> extProcStreamState) {
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

  /**
   * Transitions ExtProcStreamState to FAILED.
   */
  public static boolean markExtProcStreamFailed(
      AtomicReference<ExtProcStreamState> extProcStreamState) {
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

  /**
   * Transitions DataPlaneCallState to CLOSED.
   */
  public static boolean markDataPlaneCallClosed(
      AtomicReference<DataPlaneCallState> dataPlaneCallState) {
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
}
