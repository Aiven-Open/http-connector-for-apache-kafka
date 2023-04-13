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

import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AccessTokenHttpSenderTest extends HttpSenderTestUtils<AccessTokenHttpSender> {

    @Override
    protected AccessTokenHttpSender buildHttpSender(final HttpSinkConfig config, final HttpClient client) {
        return new AccessTokenHttpSender(config, client);
    }

    @Test
    void shouldThrowExceptionWithoutConfig() {
        final Exception thrown = assertThrows(NullPointerException.class, () -> new AccessTokenHttpSender(null, null));
        assertEquals("config should not be null", thrown.getMessage());
    }

    @Override
    protected void sendMessage(final HttpSender httpSender, final String message) {
        ((AccessTokenHttpSender) httpSender).call();
    }

    @Override
    protected URI getTargetURI(final HttpSinkConfig config) {
        return config.oauth2AccessTokenUri();
    }

    @Test
    void shouldBuildDefaultAccessTokenRequest() throws Exception {
        final var expectedAuthHeader = "Basic " + Base64.getEncoder()
                                                        .encodeToString(
                                                            ("some_client_id" + ":some_client_secret").getBytes(
                                                                StandardCharsets.UTF_8));
        super.assertHttpSender(defaultConfig(), List.of("grant_type=client_credentials"),
            httpRequests -> httpRequests.forEach(httpRequest -> {
                assertThat(httpRequest.headers().firstValue(HttpRequestBuilder.HEADER_CONTENT_TYPE)).hasValue(
                    "application/x-www-form-urlencoded");
                assertThat(httpRequest.headers().firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)).hasValue(
                    expectedAuthHeader);
            }));
    }

    @Test
    void shouldBuildDefaultAccessTokenRequestWithUrl() throws Exception {
        final var configBase = new HashMap<>(defaultConfig());
        configBase.put("oauth2.client.id", "some_client_id");
        configBase.put("oauth2.client.secret", "some_client_secret");
        configBase.put("oauth2.client.authorization.mode", "url");

        super.assertHttpSender(configBase,
            List.of("grant_type=client_credentials&client_id=some_client_id&client_secret=some_client_secret"),
            httpRequests -> httpRequests.forEach(httpRequest -> {
                assertThat(httpRequest.headers().firstValue(HttpRequestBuilder.HEADER_CONTENT_TYPE)).hasValue(
                    "application/x-www-form-urlencoded");
                assertThat(httpRequest.headers().firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)).isEmpty();
            }));

    }

    @Test
    void shouldBuildCustomisedAccessTokenRequest() throws Exception {
        final Map<String, String> configBase = new HashMap<>(defaultConfig());
        configBase.put("oauth2.client.authorization.mode", "url");
        configBase.put("oauth2.grant_type.key", "type");
        configBase.put("oauth2.grant_type", "api-key");
        configBase.put("oauth2.client.id.key", "key");
        configBase.put("oauth2.client.id", "some_client_id");
        configBase.put("oauth2.client.secret.key", "secret");
        configBase.put("oauth2.client.secret", "some_client_secret");
        configBase.put("oauth2.client.scope", "scope1,scope2");

        super.assertHttpSender(configBase,
            List.of("type=api-key&scope=scope1%2Cscope2&key=some_client_id&secret" + "=some_client_secret"),
            httpRequests -> httpRequests.forEach(httpRequest -> {
                assertThat(httpRequest.headers().firstValue(HttpRequestBuilder.HEADER_CONTENT_TYPE)).hasValue(
                    "application/x-www-form-urlencoded");
                assertThat(httpRequest.headers().firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)).isEmpty();
            }));
    }

    @Override
    protected Map<String, String> defaultConfig() {
        return Map.of("http.url", "http://localhost:42", "http.authorization.type", "oauth2", "oauth2.access.token.url",
            "http://localhost:42/token", "oauth2.client.id", "some_client_id", "oauth2.client.secret",
            "some_client_secret");
    }

}
