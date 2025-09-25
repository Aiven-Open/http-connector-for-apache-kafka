/*
 * Copyright 2023 Aiven Oy and http-connector-for-apache-kafka project contributors
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

import javax.net.ssl.SSLContext;

import java.util.Map;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SslContextBuilderTest {

    @Test
    void createSslContextWithTrustAllCertificates() throws Exception {
        final var config = new HttpSinkConfig(Map.of(
            "http.url", "https://example.com",
            "http.authorization.type", "none",
            "http.ssl.trust.all.certs", "true"
        ));

        final SSLContext sslContext = SslContextBuilder.createSslContext(config);
        
        assertThat(sslContext).isNotNull();
        assertThat(sslContext.getProtocol()).isEqualTo("TLS");
    }

    @Test
    void createSslContextWithoutTruststore() throws Exception {
        final var config = new HttpSinkConfig(Map.of(
            "http.url", "https://example.com",
            "http.authorization.type", "none"
        ));

        final SSLContext sslContext = SslContextBuilder.createSslContext(config);
        
        assertThat(sslContext).isNotNull();
        assertThat(sslContext.getProtocol()).isEqualTo("TLS");
    }

    @Test
    void createSslContextWithInvalidTruststore() {
        final var config = new HttpSinkConfig(Map.of(
            "http.url", "https://example.com",
            "http.authorization.type", "none",
            "http.ssl.truststore.location", "nonexistent.jks"
        ));

        assertThatThrownBy(() -> SslContextBuilder.createSslContext(config))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Truststore file not found");
    }
}
