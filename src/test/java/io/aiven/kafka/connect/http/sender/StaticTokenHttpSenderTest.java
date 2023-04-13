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

import java.net.http.HttpClient;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StaticTokenHttpSenderTest extends HttpSenderTestUtils<StaticHttpSender> {

    @Test
    void shouldBuildDefaultStaticHttpRequest() throws Exception {
        super.assertHttpSender(defaultConfig(), List.of("some message"),
            httpRequests -> httpRequests.forEach(httpRequest -> {
                assertThat(httpRequest.headers().firstValue(HttpRequestBuilder.HEADER_CONTENT_TYPE)).isEmpty();
                assertThat(
                    httpRequest.headers().firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION).orElse(null)).isEqualTo(
                    "Bearer myToken");
            }));
    }

    @Test
    void shouldBuildCustomStaticHttpRequest() throws Exception {
        final var configBase = new HashMap<>(defaultConfig());
        configBase.put("http.headers.content.type", "application/json");
        configBase.put("http.headers.additional", "header1:value1,header2:value2");

        super.assertHttpSender(configBase, List.of("some message"),
            httpRequests -> httpRequests.forEach(httpRequest -> {
                assertThat(
                    httpRequest.headers().firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION).orElse(null)).isEqualTo(
                    "Bearer myToken");
                assertThat(
                    httpRequest.headers().firstValue(HttpRequestBuilder.HEADER_CONTENT_TYPE).orElse(null)).isEqualTo(
                    "application/json");
                assertThat(httpRequest.headers().firstValue("header1").orElse(null)).isEqualTo("value1");
                assertThat(httpRequest.headers().firstValue("header2").orElse(null)).isEqualTo("value2");
            }));

    }

    @Override
    protected StaticHttpSender buildHttpSender(final HttpSinkConfig config, final HttpClient client) {
        return new StaticHttpSender(config, client);
    }

    @Override
    protected Map<String, String> defaultConfig() {
        return Map.of("http.url", "http://localhost:42", "http.authorization.type", "static",
            "http.headers.authorization", "Bearer myToken");
    }

}


