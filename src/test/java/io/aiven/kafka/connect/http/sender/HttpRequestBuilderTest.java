/*
 * Copyright 2021 Aiven Oy and http-connector-for-apache-kafka project contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.http.sender;

import java.net.URL;
import java.util.Map;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import org.junit.jupiter.api.Test;

import static io.aiven.kafka.connect.http.sender.HttpRequestBuilder.DEFAULT_HTTP_REQUEST_BUILDER;
import static org.assertj.core.api.Assertions.assertThat;

public class HttpRequestBuilderTest {

    @Test
    void shouldFormatUpdateUrlWithKey() throws Exception {
        final var configBase = Map.of(
                "http.url", "http://localhost:42",
                "http.update.url", "http://localhost:42/Objects(${key})",
                "http.enable.update.url", "true",
                "http.authorization.type", "none"
        );
        final HttpSinkConfig config = new HttpSinkConfig(configBase);
        final var request = DEFAULT_HTTP_REQUEST_BUILDER.build(config, "12345").build();

        assertThat(request.uri()).isEqualTo(new URL("http://localhost:42/Objects(12345)").toURI());
    }

    @Test
    void shouldUseNormalUrlIfEnableUpdateUrlIsFalse() throws Exception {
        final var configBase = Map.of(
                "http.url", "http://localhost:42",
                "http.update.url", "http://localhost:42?id=${key}",
                "http.enable.update.url", "false",
                "http.authorization.type", "none"
        );
        final HttpSinkConfig config = new HttpSinkConfig(configBase);
        final var request = DEFAULT_HTTP_REQUEST_BUILDER.build(config, "12345").build();
        assertThat(request.uri()).isEqualTo(new URL("http://localhost:42").toURI());
    }

    @Test
    void shouldUseNormalUrlWithoutKey() throws Exception {
        final var configBase = Map.of(
                "http.url", "http://localhost:42",
                "http.update.url", "http://localhost:42?id=${key}",
                "http.enable.update.url", "true",
                "http.authorization.type", "none"
        );
        final HttpSinkConfig config = new HttpSinkConfig(configBase);
        final var request = DEFAULT_HTTP_REQUEST_BUILDER.build(config, null).build();

        assertThat(request.uri()).isEqualTo(new URL("http://localhost:42").toURI());

    }


}
