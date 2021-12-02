/*
 * Copyright 2021 Aiven Oy
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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AccessTokenHttpRequestBuilderTest {

    @Test
    void shouldBuildDefaultAccessTokenRequest() throws Exception {
        final var configBase = Map.of(
                "http.url", "http://localhost:42",
                "http.authorization.type", "oauth2",
                "oauth2.access.token.url", "http://localhost:42/token",
                "oauth2.client.id", "some_client_id",
                "oauth2.client.secret", "some_client_secret"
        );
        final HttpSinkConfig config = new HttpSinkConfig(configBase);
        final var accessTokenRequest =
                new AccessTokenHttpRequestBuilder().build(config).build();

        assertEquals(new URL("http://localhost:42/token").toURI(), accessTokenRequest.uri());

        final var expectedAuthHeader = "Basic "
                + Base64.getEncoder()
                    .encodeToString("some_client_id:some_client_secret".getBytes(StandardCharsets.UTF_8));

        assertEquals(
                Optional.of(Duration.ofSeconds(config.httpTimeout())),
                accessTokenRequest.timeout());
        assertEquals("POST",
                accessTokenRequest.method());
        assertEquals(Optional.of("application/x-www-form-urlencoded"),
                accessTokenRequest.headers().firstValue(HttpRequestBuilder.HEADER_CONTENT_TYPE));
        assertEquals(Optional.of(expectedAuthHeader),
                accessTokenRequest.headers().firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION));

    }

    @Test
    void shouldBuildCustomisedAccessTokenRequest() throws Exception {
        final var configBase = Map.of(
                "http.url", "http://localhost:42",
                "http.authorization.type", "oauth2",
                "oauth2.access.token.url", "http://localhost:42/token",
                "oauth2.client.id", "some_client_id",
                "oauth2.client.secret", "some_client_secret",
                "oauth2.client.authorization.mode", "url",
                "oauth2.client.scope", "scope1,scope2"
        );
        final HttpSinkConfig config = new HttpSinkConfig(configBase);
        final var accessTokenRequest =
                new AccessTokenHttpRequestBuilder().build(config).build();

        assertEquals(new URL("http://localhost:42/token").toURI(), accessTokenRequest.uri());

        assertEquals(
                Optional.of(Duration.ofSeconds(config.httpTimeout())),
                accessTokenRequest.timeout());
        assertEquals("POST",
                accessTokenRequest.method());
        assertEquals(Optional.of("application/x-www-form-urlencoded"),
                accessTokenRequest.headers().firstValue(HttpRequestBuilder.HEADER_CONTENT_TYPE));
        assertTrue(accessTokenRequest.headers().firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION).isEmpty());
    }

}
