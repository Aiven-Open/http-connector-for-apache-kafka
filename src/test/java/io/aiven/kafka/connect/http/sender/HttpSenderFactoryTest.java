/*
 * Copyright 2024 Aiven Oy and http-connector-for-apache-kafka project contributors
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

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HttpSenderFactoryTest {

    @Test
    void testDisabledHostnameVerification() throws IOException, InterruptedException {
        final var config = new HttpSinkConfig(Map.of(
            "http.url", "https://wrong.host.badssl.com/",
            "http.authorization.type", "none",
            "http.ssl.trust.all.certs", "true"
        ));
        final var httpSender = HttpSenderFactory.buildHttpClient(config);
        final HttpRequest request = HttpRequest.newBuilder().GET()
            .uri(config.httpUri())
            .build();
        final var response = httpSender.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
    }

    @Test
    void testHttpClientWithTruststoreConfiguration() {
        final var config = new HttpSinkConfig(Map.of(
            "http.url", "https://example.com",
            "http.authorization.type", "none",
            "ssl.truststore.location", "truststore.jks",
            "ssl.truststore.password", "password"
        ));

        final var httpClient = HttpSenderFactory.buildHttpClient(config);
        assertThat(httpClient).isNotNull();
    }

    @Test
    void testHttpClientWithoutSslConfiguration() {
        final var config = new HttpSinkConfig(Map.of(
            "http.url", "http://example.com",
            "http.authorization.type", "none"
        ));

        final var httpClient = HttpSenderFactory.buildHttpClient(config);
        assertThat(httpClient).isNotNull();
    }
}
