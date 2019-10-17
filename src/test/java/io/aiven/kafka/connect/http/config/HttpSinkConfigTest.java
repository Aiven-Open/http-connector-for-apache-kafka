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
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class HttpSinkConfigTest {
    @Test
    void requiredConfigurations() {
        final Map<String, String> properties = Map.of();
        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new HttpSinkConfig(properties));
        assertEquals("Missing required configuration \"http.url\" which has no default value.", t.getMessage());
    }

    @Test
    void correctMinimalConfig() throws MalformedURLException {
        final Map<String, String> properties = Map.of(
            "http.url", "http://localhost:8090",
            "http.authorization.type", "none"
        );

        final HttpSinkConfig config = new HttpSinkConfig(properties);
        assertEquals(new URL("http://localhost:8090"), config.httpUrl());
        assertEquals(AuthorizationType.NONE, config.authorizationType());
        assertNull(config.headerContentType());
        assertEquals(1, config.maxRetries());
        assertEquals(3000, config.retryBackoffMs());
        assertEquals(10_000, config.maxOutstandingRecords());
    }

    @Test
    void invalidUrl() throws MalformedURLException {
        final Map<String, String> properties = Map.of(
            "http.url", "#http://localhost:8090",
            "http.authorization.type", "none"
        );

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new HttpSinkConfig(properties));
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
    @ValueSource(strings = {"none", "static"})
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
                + "supported values are: 'none', 'static'",
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
    void negativeMaxOutstandingRecords() {
        final Map<String, String> properties = Map.of(
            "http.url", "http://localhost:8090",
            "http.authorization.type", "none",
            "max.outstanding.records", "-1"
        );

        final Throwable t = assertThrows(
            ConfigException.class, () -> new HttpSinkConfig(properties)
        );
        assertEquals("Invalid value -1 for configuration max.outstanding.records: Value must be at least 0",
            t.getMessage());
    }

    @Test
    void correctMaxOutstandingRecords() {
        final Map<String, String> properties = Map.of(
            "http.url", "http://localhost:8090",
            "http.authorization.type", "none",
            "max.outstanding.records", "12345"
        );

        final HttpSinkConfig config = new HttpSinkConfig(properties);
        assertEquals(12345, config.maxOutstandingRecords());
    }
}
