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

package io.grpc.xds;

import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import io.envoyproxy.envoy.config.core.v3.HeaderMap;
import io.grpc.Metadata;
import io.grpc.xds.ExternalProcessorFilter.HeaderForwardingRulesConfig;
import java.util.Locale;
import java.util.Optional;

final class ExternalProcessorHeaderUtil {
  private ExternalProcessorHeaderUtil() {}

  static HeaderMap toHeaderMap(
      Metadata metadata, Optional<HeaderForwardingRulesConfig> forwardRules) {
    HeaderMap.Builder builder = HeaderMap.newBuilder();

    for (String key : metadata.keys()) {
      if (forwardRules.isPresent() && !forwardRules.get().isAllowed(key)) {
        continue;
      }
      // Map binary headers using raw_value
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
}
