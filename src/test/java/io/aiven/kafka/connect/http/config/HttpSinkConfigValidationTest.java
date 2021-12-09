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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class HttpSinkConfigValidationTest {

    private static final String CONTENT_TYPE_VALUE = "application/json";

    @Test
    void recommendedValuesForAuthorization() {
        final Map<String, String> properties = Map.of(
            "http.url", "http://localhost:8090"
        );

        final var v = HttpSinkConfig.configDef().validate(properties).stream()
            .filter(x -> x.name().equals("http.authorization.type"))
            .findFirst().get();
        assertIterableEquals(
            AuthorizationType.NAMES,
            v.recommendedValues()
        );
    }

    @Test
    void checkAdditionalHeadersValidation() {
        final Map<String, String> properties = new HashMap<>(Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "http.headers.additional", "test:value,test:valueUpperCase"
        ));

        assertThrows(
            ConfigException.class,
            () -> new HttpSinkConfig(properties),
            "Expected config exception due to repeated keys, but it parsed successfully"
        );

        properties.replace("http.headers.additional", "test:value,wrong,test1:test1");

        assertThrows(
            ConfigException.class,
            () -> new HttpSinkConfig(properties),
            "Expected config exception due to header without value, but it parsed successfully"
        );

        properties.replace("http.headers.additional", "test:value,TEST:valueUpperCase");
        assertThrows(
            ConfigException.class,
            () -> new HttpSinkConfig(properties),
            "Expected config exception due to repeated case-insensitive keys, but it parsed successfully"
        );

        properties.replace("http.headers.additional", "test:value,,test2:test2");
        assertThrows(
            ConfigException.class,
            () -> new HttpSinkConfig(properties),
            "Expected config exception due to empty value, but it parsed successfully"
        );
    }

    @Test
    void checkContentTypeValidation() {
        final Map<String, String> properties = new HashMap<>(Map.of(
            "http.url", "http://localhost:8090",
            "http.authorization.type", "none",
            "http.headers.content.type", CONTENT_TYPE_VALUE
        ));

        assertDoesNotThrow(
            () -> new HttpSinkConfig(properties),
            "Expected config valid due to valid content type"
        );

        properties.replace("http.headers.content.type", "");
        assertThrows(
            ConfigException.class,
            () -> new HttpSinkConfig(properties),
            "Expected config exception due to empty content type"
        );

        properties.replace("http.headers.content.type", "        ");
        assertThrows(
            ConfigException.class,
            () -> new HttpSinkConfig(properties),
            "Expected config exception due to blank content type"
        );

        properties.replace("http.headers.content.type", " \r\n       ");
        assertThrows(
            ConfigException.class,
            () -> new HttpSinkConfig(properties),
            "Expected config exception due to blank content type"
        );
    }
}
