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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.ConnectException;

public class HttpSinkConfig extends AbstractConfig {
    private static final String GROUP_CONNECTION = "Connection";
    public static final String HTTP_URL_CONFIG = "http.url";
    public static final String HTTP_AUTHORIZATION_TYPE_CONFIG = "http.authorization.type";

    public static final String HTTP_HEADERS_AUTHORIZATION_CONFIG = "http.headers.authorization";
    public static final String HTTP_HEADERS_CONTENT_TYPE_CONFIG = "http.headers.content.type";

    public static final String NAME_CONFIG = "name";

    public static ConfigDef configDef() {
        final ConfigDef configDef = new ConfigDef();
        addConnectionConfigGroup(configDef);
        return configDef;
    }

    private static void addConnectionConfigGroup(final ConfigDef configDef) {
        int connectionGroupCounter = 0;
        configDef.define(
            HTTP_URL_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            new ConfigDef.Validator() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    if (value == null) {
                        throw new ConfigException(HTTP_URL_CONFIG, value, "can't be null");
                    }
                    if (!(value instanceof String)) {
                        throw new ConfigException(HTTP_URL_CONFIG, value, "must be string");
                    }
                    try {
                        new URL((String) value);
                    } catch (final MalformedURLException e) {
                        throw new ConfigException(HTTP_URL_CONFIG, value, "malformed URL");
                    }
                }
            },
            ConfigDef.Importance.HIGH,
            "The URL to send data to.",
            GROUP_CONNECTION,
            connectionGroupCounter++,
            ConfigDef.Width.LONG,
            HTTP_URL_CONFIG
        );

        final String supportedAuthorizationTypes = AuthorizationType.names().stream()
            .map(f -> "'" + f + "'")
            .collect(Collectors.joining(", "));
        configDef.define(
            HTTP_AUTHORIZATION_TYPE_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            new ConfigDef.Validator() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    if (value == null) {
                        throw new ConfigException(HTTP_AUTHORIZATION_TYPE_CONFIG, value);
                    }
                    assert value instanceof String;
                    final String valueStr = (String) value;
                    if (!AuthorizationType.names().contains(valueStr)) {
                        throw new ConfigException(
                            HTTP_AUTHORIZATION_TYPE_CONFIG, valueStr,
                            "supported values are: " + supportedAuthorizationTypes);
                    }
                }
            },
            ConfigDef.Importance.HIGH,
            "The HTTP authorization type. "
                + "The supported values are: " + supportedAuthorizationTypes + ".",
            GROUP_CONNECTION,
            connectionGroupCounter++,
            ConfigDef.Width.SHORT,
            HTTP_AUTHORIZATION_TYPE_CONFIG,
            List.of(HTTP_HEADERS_AUTHORIZATION_CONFIG),
            FixedSetRecommender.ofSupportedValues(AuthorizationType.names())
        );

        configDef.define(
            HTTP_HEADERS_AUTHORIZATION_CONFIG,
            ConfigDef.Type.PASSWORD,
            null,
            ConfigDef.Importance.MEDIUM,
            "The static content of Authorization header. "
                + "Must be set along with 'static' authorization type.",
            GROUP_CONNECTION,
            connectionGroupCounter++,
            ConfigDef.Width.MEDIUM,
            HTTP_HEADERS_AUTHORIZATION_CONFIG,
            new ConfigDef.Recommender() {
                @Override
                public List<Object> validValues(final String name, final Map<String, Object> parsedConfig) {
                    return List.of();
                }

                @Override
                public boolean visible(final String name, final Map<String, Object> parsedConfig) {
                    return AuthorizationType.STATIC.name.equalsIgnoreCase(
                        (String) parsedConfig.get(HTTP_AUTHORIZATION_TYPE_CONFIG));
                }
            });

        configDef.define(
            HTTP_HEADERS_CONTENT_TYPE_CONFIG,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.LOW,
            "The value of Content-Type that will be send with each request.",
            GROUP_CONNECTION,
            connectionGroupCounter++,
            ConfigDef.Width.MEDIUM,
            HTTP_HEADERS_CONTENT_TYPE_CONFIG
        );
    }

    public HttpSinkConfig(final Map<String, String> properties) {
        super(configDef(), properties);
        validate();
    }

    private void validate() {
        switch (authorizationType()) {
            case STATIC:
                if (headerAuthorization() == null || headerAuthorization().isBlank()) {
                    throw new ConfigException(
                        HTTP_HEADERS_AUTHORIZATION_CONFIG,
                        getPassword(HTTP_HEADERS_AUTHORIZATION_CONFIG),
                        "Must be present when " + HTTP_HEADERS_CONTENT_TYPE_CONFIG
                            + " = " + AuthorizationType.STATIC);
                }
                break;

            case NONE:
                if (headerAuthorization() != null && !headerAuthorization().isBlank()) {
                    throw new ConfigException(
                        HTTP_HEADERS_AUTHORIZATION_CONFIG,
                        getPassword(HTTP_HEADERS_AUTHORIZATION_CONFIG),
                        "Must not be present when " + HTTP_HEADERS_CONTENT_TYPE_CONFIG
                            + " != " + AuthorizationType.STATIC);
                }
                break;

            default:
                break;
        }
    }

    public final URL httpUrl() {
        try {
            return new URL(getString(HTTP_URL_CONFIG));
        } catch (final MalformedURLException e) {
            throw new ConnectException(e);
        }
    }

    public AuthorizationType authorizationType() {
        return AuthorizationType.forName(getString(HTTP_AUTHORIZATION_TYPE_CONFIG));
    }

    public final String headerAuthorization() {
        final Password authPasswd = getPassword(HTTP_HEADERS_AUTHORIZATION_CONFIG);
        return authPasswd != null ? authPasswd.value() : null;
    }

    public final String headerContentType() {
        return getString(HTTP_HEADERS_CONTENT_TYPE_CONFIG);
    }

    public final int batchSize() {
        return 1;
    }

    public final String connectorName() {
        return originalsStrings().get(NAME_CONFIG);
    }
}
