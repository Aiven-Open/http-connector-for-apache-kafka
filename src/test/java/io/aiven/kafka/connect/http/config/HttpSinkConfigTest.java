/*
 * Copyright 2019 Aiven Oy
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

package io.aiven.kafka.connect.http.config;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class HttpSinkConfigTest {

    @Test
    void requiredConfigurations() {
        final Map<String, String> properties = Map.of();
        final Throwable t = assertThrows(
                ConfigException.class, () -> new HttpSinkConfig(properties));
        assertEquals("Missing required configuration \"http.url\" which has no default value.", t.getMessage());
    }

    @Test
    void correctMinimalConfig() throws MalformedURLException, URISyntaxException {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none"
        );

        final HttpSinkConfig config = new HttpSinkConfig(properties);
        assertEquals(new URL("http://localhost:8090").toURI(), config.httpUri());
        assertEquals(AuthorizationType.NONE, config.authorizationType());
        assertNull(config.headerContentType());
        assertFalse(config.batchingEnabled());
        assertEquals(500, config.batchMaxSize());
        assertEquals(1, config.maxRetries());
        assertEquals(3000, config.retryBackoffMs());
        assertEquals(Collections.emptyMap(), config.getAdditionalHeaders());
        assertNull(config.oauth2AccessTokenUri());
        assertNull(config.oauth2ClientId());
        assertNull(config.oauth2ClientSecret());
        assertNull(config.oauth2ClientScope());
        assertEquals(OAuth2AuthorizationMode.HEADER, config.oauth2AuthorizationMode());
        assertEquals("access_token", config.oauth2ResponseTokenProperty());
        assertNull(config.kafkaRetryBackoffMs());
    }

    @Test
    void invalidOAuth2AccessTokenUrl() {
        final var emptyAccessTokenUrlConfig = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "oauth2.access.token.url", ""
        );

        final var emptyStringT =
                assertThrows(ConfigException.class, () -> new HttpSinkConfig(emptyAccessTokenUrlConfig));
        assertEquals(
                "Invalid value  for configuration oauth2.access.token.url: malformed URL",
                emptyStringT.getMessage()
        );

        final var wrongAccessTokenUrlConfig = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "oauth2.access.token.url", ";http://localhost:8090"
        );

        final var wrongUrlT =
                assertThrows(ConfigException.class, () -> new HttpSinkConfig(wrongAccessTokenUrlConfig));
        assertEquals(
                "Invalid value ;http://localhost:8090 for configuration "
                        + "oauth2.access.token.url: malformed URL",
                wrongUrlT.getMessage()
        );
    }

    @Test
    void invalidOAuth2ClientAuthorizationMode() {
        final var invalidClientAuthorizationModeConfig = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "oauth2",
                "oauth2.access.token.url", "http://localhost:8090/token",
                "oauth2.client.id", "client_id",
                "oauth2.client.secret", "client_secret",
                "oauth2.client.authorization.mode", "AAAABBBCCCC"
        );

        final var t =
                assertThrows(ConfigException.class, () -> new HttpSinkConfig(invalidClientAuthorizationModeConfig));
        assertEquals(
                "Invalid value AAAABBBCCCC for configuration oauth2.client.authorization.mode: "
                        + "supported values are: [HEADER, URL]",
                t.getMessage()
        );
    }

    @Test
    void invalidOAuth2Configuration() {
        final var noAccessTokenUrlConfig = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "oauth2"
        );

        final var noAccessTokenUrlE =
                assertThrows(ConfigException.class, () -> new HttpSinkConfig(noAccessTokenUrlConfig));
        assertEquals(
                "Invalid value null for configuration oauth2.access.token.url: "
                        + "Must be present when http.headers.content.type = OAUTH2",
                noAccessTokenUrlE.getMessage());

        final var noSecretIdConfig = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "oauth2",
                "oauth2.access.token.url", "http://localhost:8090/token"
        );

        final var noSecretIdConfigE =
                assertThrows(ConfigException.class, () -> new HttpSinkConfig(noSecretIdConfig));
        assertEquals(
                "Invalid value null for configuration oauth2.client.id: "
                        + "Must be present when http.headers.content.type = OAUTH2",
                noSecretIdConfigE.getMessage());

        final var noSecretConfig = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "oauth2",
                "oauth2.access.token.url", "http://localhost:8090/token",
                "oauth2.client.id", "client_id"
        );

        final var noSecretConfigE =
                assertThrows(ConfigException.class, () -> new HttpSinkConfig(noSecretConfig));
        assertEquals(
                "Invalid value null for configuration oauth2.client.secret: "
                        + "Must be present when http.headers.content.type = OAUTH2",
                noSecretConfigE.getMessage());

    }

    @Test
    void validOAuth2MinimalConfiguration() throws MalformedURLException, URISyntaxException {

        final var oauth2Config = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "oauth2",
                "oauth2.access.token.url", "http://localhost:8090/token",
                "oauth2.client.id", "client_id",
                "oauth2.client.secret", "client_secret"
        );

        final var config = new HttpSinkConfig(oauth2Config);

        assertEquals(new URL("http://localhost:8090/token").toURI(), config.oauth2AccessTokenUri());
        assertEquals("client_id", config.oauth2ClientId());
        assertEquals("client_secret", config.oauth2ClientSecret().value());
        assertEquals(OAuth2AuthorizationMode.HEADER, config.oauth2AuthorizationMode());
        assertEquals("access_token", config.oauth2ResponseTokenProperty());
        assertNull(config.oauth2ClientScope());
    }

    @Test
    void validOAuth2FullConfiguration() throws MalformedURLException, URISyntaxException {

        final var oauth2Config = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "oauth2",
                "oauth2.access.token.url", "http://localhost:8090/token",
                "oauth2.client.id", "client_id",
                "oauth2.client.secret", "client_secret",
                "oauth2.client.authorization.mode", "url",
                "oauth2.client.scope", "scope1,scope2",
                "oauth2.response.token.property", "moooooo"
        );

        final var config = new HttpSinkConfig(oauth2Config);

        assertEquals(new URL("http://localhost:8090/token").toURI(), config.oauth2AccessTokenUri());
        assertEquals("client_id", config.oauth2ClientId());
        assertEquals("client_secret", config.oauth2ClientSecret().value());
        assertEquals(OAuth2AuthorizationMode.URL, config.oauth2AuthorizationMode());
        assertEquals("moooooo", config.oauth2ResponseTokenProperty());
        assertEquals("scope1,scope2", config.oauth2ClientScope());
    }

    @Test
    void invalidUrl() {
        final Map<String, String> properties = Map.of(
                "http.url", "#http://localhost:8090",
                "http.authorization.type", "none"
        );

        final Throwable t = assertThrows(
                ConfigException.class, () -> new HttpSinkConfig(properties));
        assertEquals("Invalid value #http://localhost:8090 for configuration http.url: malformed URL",
                t.getMessage());
    }

    @Test
    void missingAuthorizationType() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090"
        );

        final Throwable t = assertThrows(ConfigException.class, () -> new HttpSinkConfig(properties));
        assertEquals("Missing required configuration \"http.authorization.type\" which has no default value.",
                t.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "static", "oauth2"})
    void supportedAuthorizationType(final String authorization) {
        Map<String, String> properties = Map.of(
            "http.url", "http://localhost:8090",
            "http.authorization.type", authorization
        );

        final AuthorizationType expectedAuthorizationType;
        if ("none".equals(authorization)) {
            expectedAuthorizationType = AuthorizationType.NONE;
        } else if ("static".equals(authorization)) {
            expectedAuthorizationType = AuthorizationType.STATIC;
            properties = new HashMap<>(properties);
            properties.put("http.headers.authorization", "some");
        } else if ("oauth2".equals(authorization)) {
            expectedAuthorizationType = AuthorizationType.OAUTH2;
            properties = new HashMap<>(properties);
            properties.put("oauth2.access.token.url", "http://localhost:42");
            properties.put("oauth2.client.id", "client_id");
            properties.put("oauth2.client.secret", "client_secret");
        } else {
            throw new RuntimeException("Shouldn't be here");
        }

        final HttpSinkConfig config = new HttpSinkConfig(properties);

        assertEquals(expectedAuthorizationType, config.authorizationType());
    }

    @Test
    void unsupportedAuthorizationType() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "unsupported"
        );

        final Throwable t = assertThrows(
                ConfigException.class, () -> new HttpSinkConfig(properties)
        );
        assertEquals("Invalid value unsupported for configuration http.authorization.type: "
                        + "supported values are: [none, oauth2, static]",
                t.getMessage());
    }

    @Test
    void missingAuthorizationHeaderWhenAuthorizationTypeStatic() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "static"
        );

        final Throwable t = assertThrows(
                ConfigException.class, () -> new HttpSinkConfig(properties)
        );
        assertEquals("Invalid value null for configuration http.headers.authorization: "
                        + "Must be present when http.headers.content.type = STATIC",
                t.getMessage());
    }

    @Test
    void presentAuthorizationHeaderWhenAuthorizationTypeNotStatic() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "http.headers.authorization", "some"
        );

        final Throwable t = assertThrows(
                ConfigException.class, () -> new HttpSinkConfig(properties)
        );
        assertEquals("Invalid value [hidden] for configuration http.headers.authorization: "
                        + "Must not be present when http.headers.content.type != STATIC",
                t.getMessage());
    }

    @Test
    void headerContentType() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "http.headers.content.type", "application/json"
        );

        final HttpSinkConfig config = new HttpSinkConfig(properties);
        assertEquals("application/json", config.headerContentType());
    }

    @Test
    void headerAdditionalFields() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "http.headers.additional", "test:value,test2:value2,test4:value4"
        );

        final HttpSinkConfig config = new HttpSinkConfig(properties);
        final var additionalHeaders = config.getAdditionalHeaders();

        assertEquals(3, additionalHeaders.size());
        assertEquals("value", additionalHeaders.get("test"));
        assertEquals("value2", additionalHeaders.get("test2"));
        assertEquals("value4", additionalHeaders.get("test4"));
    }

    @Test
    void headerAdditionalField() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "http.headers.additional", "test:value"
        );

        final HttpSinkConfig config = new HttpSinkConfig(properties);
        final var additionalHeaders = config.getAdditionalHeaders();

        assertEquals(1, additionalHeaders.size());
        assertEquals("value", additionalHeaders.get("test"));
    }

    @Test
    void correctBatching() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "batching.enabled", "true",
                "batch.max.size", "123456"
        );

        final HttpSinkConfig config = new HttpSinkConfig(properties);
        assertTrue(config.batchingEnabled());
        assertEquals(123456, config.batchMaxSize());
    }

    @Test
    void tooBigBatchSize() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "batching.enabled", "true",
                "batch.max.size", "1000001"
        );

        final Throwable t = assertThrows(
                ConfigException.class, () -> new HttpSinkConfig(properties)
        );
        assertEquals("Invalid value 1000001 for configuration batch.max.size: Value must be no more than 1000000",
                t.getMessage());
    }

    @Test
    void negativeMaxRetries() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "max.retries", "-1"
        );

        final Throwable t = assertThrows(
                ConfigException.class, () -> new HttpSinkConfig(properties)
        );
        assertEquals("Invalid value -1 for configuration max.retries: Value must be at least 0",
                t.getMessage());
    }

    @Test
    void correctMaxRetries() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "max.retries", "123"
        );

        final HttpSinkConfig config = new HttpSinkConfig(properties);
        assertEquals(123, config.maxRetries());
    }

    @Test
    void negativeRetryBackoffMs() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "retry.backoff.ms", "-1"
        );

        final Throwable t = assertThrows(
                ConfigException.class, () -> new HttpSinkConfig(properties)
        );
        assertEquals("Invalid value -1 for configuration retry.backoff.ms: Value must be at least 0",
                t.getMessage());
    }

    @Test
    void tooBigKafkaRetryBackoffMs() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "kafka.retry.backoff.ms", String.valueOf(TimeUnit.HOURS.toMillis(25))
        );

        final Throwable t = assertThrows(
                ConfigException.class, () -> new HttpSinkConfig(properties)
        );
        assertEquals("Invalid value 90000000 for configuration kafka.retry.backoff.ms: "
                        + "Value must be no more than 86400000 (24 hours)",
                t.getMessage());
    }


    @Test
    void negativeKafkaRetryBackoffMs() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "kafka.retry.backoff.ms", "-1"
        );

        final Throwable t = assertThrows(
                ConfigException.class, () -> new HttpSinkConfig(properties)
        );
        assertEquals("Invalid value -1 for configuration kafka.retry.backoff.ms: Value must be at least 0",
                t.getMessage());
    }

    @Test
    void correctRetryBackoffMs() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "retry.backoff.ms", "12345"
        );

        final HttpSinkConfig config = new HttpSinkConfig(properties);
        assertEquals(12345, config.retryBackoffMs());
    }

    @Test
    void customKafkaRetryBackoffMs() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "kafka.retry.backoff.ms", "6000"
        );

        final var config = new HttpSinkConfig(properties);
        assertEquals(6000, config.kafkaRetryBackoffMs());
    }

}
