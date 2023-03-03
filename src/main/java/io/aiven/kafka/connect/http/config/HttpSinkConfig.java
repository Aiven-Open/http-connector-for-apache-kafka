/*
 * Copyright 2019 Aiven Oy and http-connector-for-apache-kafka project contributors
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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.ConnectException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class HttpSinkConfig extends AbstractConfig {
    private static final String CONNECTION_GROUP = "Connection";
    private static final String HTTP_URL_CONFIG = "http.url";

    private static final String HTTP_AUTHORIZATION_TYPE_CONFIG = "http.authorization.type";
    private static final String HTTP_HEADERS_AUTHORIZATION_CONFIG = "http.headers.authorization";
    private static final String HTTP_HEADERS_CONTENT_TYPE_CONFIG = "http.headers.content.type";
    private static final String HTTP_HEADERS_ADDITIONAL = "http.headers.additional";
    private static final String HTTP_HEADERS_ADDITIONAL_DELIMITER = ":";

    public static final String KAFKA_RETRY_BACKOFF_MS_CONFIG = "kafka.retry.backoff.ms";

    private static final String OAUTH2_ACCESS_TOKEN_URL_CONFIG = "oauth2.access.token.url";
    private static final String OAUTH2_CLIENT_ID_CONFIG = "oauth2.client.id";
    private static final String OAUTH2_CLIENT_SECRET_CONFIG = "oauth2.client.secret";
    private static final String OAUTH2_CLIENT_AUTHORIZATION_MODE_CONFIG = "oauth2.client.authorization.mode";
    private static final String OAUTH2_CLIENT_SCOPE_CONFIG = "oauth2.client.scope";
    private static final String OAUTH2_RESPONSE_TOKEN_PROPERTY_CONFIG = "oauth2.response.token.property";
    private static final String BATCHING_GROUP = "Batching";
    private static final String BATCHING_ENABLED_CONFIG = "batching.enabled";
    private static final String BATCH_MAX_SIZE_CONFIG = "batch.max.size";
    private static final String BATCH_PREFIX_CONFIG = "batch.prefix";
    private static final String BATCH_PREFIX_DEFAULT = "";
    private static final String BATCH_SUFFIX_CONFIG = "batch.suffix";
    private static final String BATCH_SUFFIX_DEFAULT = "\n";
    private static final String BATCH_SEPARATOR_CONFIG = "batch.separator";
    private static final String BATCH_SEPARATOR_DEFAULT = "\n";

    private static final String DELIVERY_GROUP = "Delivery";
    private static final String MAX_RETRIES_CONFIG = "max.retries";
    private static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";

    private static final String TIMEOUT_GROUP = "Timeout";
    private static final String HTTP_TIMEOUT_CONFIG = "http.timeout";

    public static final String NAME_CONFIG = "name";

    private static final String ERRORS_GROUP = "Errors Handling";
    private static final String ERRORS_TOLERANCE = "errors.tolerance";

    public static ConfigDef configDef() {
        final ConfigDef configDef = new ConfigDef();
        addConnectionConfigGroup(configDef);
        addBatchingConfigGroup(configDef);
        addRetriesConfigGroup(configDef);
        addTimeoutConfigGroup(configDef);
        addErrorsConfigGroup(configDef);
        return configDef;
    }

    private static void addConnectionConfigGroup(final ConfigDef configDef) {
        int groupCounter = 0;
        configDef.define(
            HTTP_URL_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            new UrlValidator(),
            ConfigDef.Importance.HIGH,
            "The URL to send data to.",
            CONNECTION_GROUP,
            groupCounter++,
            ConfigDef.Width.LONG,
            HTTP_URL_CONFIG
        );

        configDef.define(
            HTTP_AUTHORIZATION_TYPE_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            new ConfigDef.Validator() {
                @Override
                @SuppressFBWarnings("NP_LOAD_OF_KNOWN_NULL_VALUE") // Suppress the ConfigException with null value.
                public void ensureValid(final String name, final Object value) {
                    if (value == null) {
                        throw new ConfigException(HTTP_AUTHORIZATION_TYPE_CONFIG, value);
                    }
                    assert value instanceof String;
                    final String valueStr = (String) value;
                    if (!AuthorizationType.NAMES.contains(valueStr)) {
                        throw new ConfigException(
                            HTTP_AUTHORIZATION_TYPE_CONFIG, valueStr,
                            "supported values are: " + AuthorizationType.NAMES);
                    }
                }

                @Override
                public String toString() {
                    return AuthorizationType.NAMES.toString();
                }
            },
            ConfigDef.Importance.HIGH,
            "The HTTP authorization type.",
            CONNECTION_GROUP,
            groupCounter++,
            ConfigDef.Width.SHORT,
            HTTP_AUTHORIZATION_TYPE_CONFIG,
            List.of(HTTP_HEADERS_AUTHORIZATION_CONFIG),
            FixedSetRecommender.ofSupportedValues(AuthorizationType.NAMES)
        );

        configDef.define(
            HTTP_HEADERS_AUTHORIZATION_CONFIG,
            ConfigDef.Type.PASSWORD,
            null,
            ConfigDef.Importance.MEDIUM,
            "The static content of Authorization header. "
                + "Must be set along with 'static' authorization type.",
            CONNECTION_GROUP,
            groupCounter++,
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
            new NonBlankStringValidator(true),
            ConfigDef.Importance.LOW,
            "The value of Content-Type that will be send with each request. Must be non-blank.",
            CONNECTION_GROUP,
            groupCounter++,
            ConfigDef.Width.MEDIUM,
            HTTP_HEADERS_CONTENT_TYPE_CONFIG
        );

        configDef.define(
                HTTP_HEADERS_ADDITIONAL,
                ConfigDef.Type.LIST,
                Collections.EMPTY_LIST,
                new KeyValuePairListValidator(HTTP_HEADERS_ADDITIONAL_DELIMITER),
                ConfigDef.Importance.LOW,
                "Additional headers to forward in the http request in the format header:value separated by a comma, "
                        + "headers are case-insensitive and no duplicate headers are allowed.",
                CONNECTION_GROUP,
                groupCounter++,
                ConfigDef.Width.MEDIUM,
                HTTP_HEADERS_ADDITIONAL
        );

        configDef.define(
                OAUTH2_ACCESS_TOKEN_URL_CONFIG,
                ConfigDef.Type.STRING,
                null,
                new UrlValidator(true),
                ConfigDef.Importance.HIGH,
                "The URL to be used for fetching an access token. "
                        + "Client Credentials is the only supported grant type.",
                CONNECTION_GROUP,
                groupCounter++,
                ConfigDef.Width.LONG,
                OAUTH2_ACCESS_TOKEN_URL_CONFIG,
                List.of(OAUTH2_CLIENT_ID_CONFIG, OAUTH2_CLIENT_SECRET_CONFIG,
                        OAUTH2_CLIENT_AUTHORIZATION_MODE_CONFIG, OAUTH2_CLIENT_SCOPE_CONFIG,
                        OAUTH2_RESPONSE_TOKEN_PROPERTY_CONFIG)
        );
        configDef.define(
                OAUTH2_CLIENT_ID_CONFIG,
                ConfigDef.Type.STRING,
                null,
                new ConfigDef.NonEmptyStringWithoutControlChars() {
                    @Override
                    public String toString() {
                        return "OAuth2 client id";
                    }
                },
                ConfigDef.Importance.HIGH,
                "The client id used for fetching an access token.",
                CONNECTION_GROUP,
                groupCounter++,
                ConfigDef.Width.LONG,
                OAUTH2_CLIENT_ID_CONFIG,
                List.of(OAUTH2_ACCESS_TOKEN_URL_CONFIG, OAUTH2_CLIENT_SECRET_CONFIG,
                        OAUTH2_CLIENT_AUTHORIZATION_MODE_CONFIG,
                        OAUTH2_CLIENT_SCOPE_CONFIG, OAUTH2_RESPONSE_TOKEN_PROPERTY_CONFIG)
        );
        configDef.define(
                OAUTH2_CLIENT_SECRET_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                ConfigDef.Importance.HIGH,
                "The secret used for fetching an access token.",
                CONNECTION_GROUP,
                groupCounter++,
                ConfigDef.Width.LONG,
                OAUTH2_CLIENT_SECRET_CONFIG,
                List.of(OAUTH2_ACCESS_TOKEN_URL_CONFIG, OAUTH2_CLIENT_ID_CONFIG,
                        OAUTH2_CLIENT_AUTHORIZATION_MODE_CONFIG,
                        OAUTH2_CLIENT_SCOPE_CONFIG, OAUTH2_RESPONSE_TOKEN_PROPERTY_CONFIG)
        );
        configDef.define(
                OAUTH2_CLIENT_AUTHORIZATION_MODE_CONFIG,
                ConfigDef.Type.STRING,
                OAuth2AuthorizationMode.HEADER.name(),
                new ConfigDef.Validator() {
                    @Override
                    public void ensureValid(final String name, final Object value) {
                        if (value == null) {
                            throw new ConfigException(name, null, "can't be null");
                        }
                        if (!(value instanceof String)) {
                            throw new ConfigException(name, value, "must be string");
                        }
                        if (!OAuth2AuthorizationMode.OAUTH2_AUTHORIZATION_MODES
                                .contains(value.toString().toUpperCase())) {
                            throw new ConfigException(
                                    OAUTH2_CLIENT_AUTHORIZATION_MODE_CONFIG, value,
                                    "supported values are: " + OAuth2AuthorizationMode.OAUTH2_AUTHORIZATION_MODES);
                        }
                    }

                    @Override
                    public String toString() {
                        return String.join(",", OAuth2AuthorizationMode.OAUTH2_AUTHORIZATION_MODES);
                    }
                },
                ConfigDef.Importance.MEDIUM,
                "Specifies how to encode ``client_id`` and ``client_secret`` in the OAuth2 authorization request. "
                        + "If set to ``header``, the credentials are encoded as an "
                        + "``Authorization: Basic <base-64 encoded client_id:client_secret>`` HTTP header. "
                        + "If set to ``url``, then ``client_id`` and ``client_secret`` "
                        + "are sent as URL encoded parameters. Default is ``header``.",
                CONNECTION_GROUP,
                groupCounter++,
                ConfigDef.Width.LONG,
                OAUTH2_CLIENT_AUTHORIZATION_MODE_CONFIG,
                List.of(OAUTH2_ACCESS_TOKEN_URL_CONFIG, OAUTH2_CLIENT_ID_CONFIG, OAUTH2_CLIENT_SECRET_CONFIG,
                        OAUTH2_CLIENT_SCOPE_CONFIG, OAUTH2_RESPONSE_TOKEN_PROPERTY_CONFIG)
        );
        configDef.define(
                OAUTH2_CLIENT_SCOPE_CONFIG,
                ConfigDef.Type.STRING,
                null,
                new ConfigDef.NonEmptyStringWithoutControlChars() {
                    @Override
                    public String toString() {
                        return "OAuth2 client scope";
                    }
                },
                ConfigDef.Importance.LOW,
                "The scope used for fetching an access token.",
                CONNECTION_GROUP,
                groupCounter++,
                ConfigDef.Width.LONG,
                OAUTH2_CLIENT_SCOPE_CONFIG,
                List.of(OAUTH2_ACCESS_TOKEN_URL_CONFIG, OAUTH2_CLIENT_ID_CONFIG, OAUTH2_CLIENT_SECRET_CONFIG,
                        OAUTH2_CLIENT_AUTHORIZATION_MODE_CONFIG, OAUTH2_RESPONSE_TOKEN_PROPERTY_CONFIG)
        );
        configDef.define(
                OAUTH2_RESPONSE_TOKEN_PROPERTY_CONFIG,
                ConfigDef.Type.STRING,
                "access_token",
                new ConfigDef.NonEmptyStringWithoutControlChars() {
                    @Override
                    public String toString() {
                        return "OAuth2 response token";
                    }
                },
                ConfigDef.Importance.LOW,
                "The name of the JSON property containing the access token returned "
                        + "by the OAuth2 provider. Default value is ``access_token``.",
                CONNECTION_GROUP,
                groupCounter++,
                ConfigDef.Width.LONG,
                OAUTH2_RESPONSE_TOKEN_PROPERTY_CONFIG,
                List.of(OAUTH2_ACCESS_TOKEN_URL_CONFIG, OAUTH2_CLIENT_ID_CONFIG, OAUTH2_CLIENT_SECRET_CONFIG,
                        OAUTH2_CLIENT_AUTHORIZATION_MODE_CONFIG, OAUTH2_CLIENT_SCOPE_CONFIG)
        );
    }

    private static void addBatchingConfigGroup(final ConfigDef configDef) {
        int groupCounter = 0;
        configDef.define(
            BATCHING_ENABLED_CONFIG,
            ConfigDef.Type.BOOLEAN,
            false,
            ConfigDef.Importance.HIGH,
            "Whether to enable batching multiple records in a single HTTP request.",
            BATCHING_GROUP,
            groupCounter++,
            ConfigDef.Width.SHORT,
            BATCHING_ENABLED_CONFIG
        );

        configDef.define(
            BATCH_MAX_SIZE_CONFIG,
            ConfigDef.Type.INT,
            500,
            ConfigDef.Range.between(1, 1_000_000),
            ConfigDef.Importance.MEDIUM,
            "The maximum size of a record batch to be sent in a single HTTP request.",
            BATCHING_GROUP,
            groupCounter++,
            ConfigDef.Width.MEDIUM,
            BATCHING_GROUP
        );

        configDef.define(
            BATCH_PREFIX_CONFIG,
            ConfigDef.Type.STRING,
            BATCH_PREFIX_DEFAULT,
            ConfigDef.Importance.HIGH,
            "Prefix added to record batches. Written once before the first record of a batch. "
                    + "Defaults to \"\" and may contain escape sequences like ``\\n``.",
            BATCHING_GROUP,
            groupCounter++,
            ConfigDef.Width.MEDIUM,
            BATCHING_GROUP
        );

        // ConfigKey automatically calls trim() on strings, but characters discarded by that method
        // are commonly used as delimiters, including our default of "\n" for suffix and separator.
        // We work around that by supplying null here the injecting the real default in the accessors.

        configDef.define(
            BATCH_SUFFIX_CONFIG,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            "Suffix added to record batches. Written once after the last record of a batch. "
                    + "Defaults to \"\\n\" (for backwards compatibility) and may contain escape sequences.",
            BATCHING_GROUP,
            groupCounter++,
            ConfigDef.Width.MEDIUM,
            BATCHING_GROUP
        );

        configDef.define(
            BATCH_SEPARATOR_CONFIG,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            "Separator for records in a batch. Defaults to \"\\n\" and may contain escape sequences.",
            BATCHING_GROUP,
            groupCounter++,
            ConfigDef.Width.MEDIUM,
            BATCHING_GROUP
        );
    }

    private static void addRetriesConfigGroup(final ConfigDef configDef) {
        int groupCounter = 0;
        configDef.define(
                KAFKA_RETRY_BACKOFF_MS_CONFIG,
                ConfigDef.Type.LONG,
                null,
                new ConfigDef.Validator() {

                    static final long MAXIMUM_BACKOFF_POLICY = 86400000; // 24 hours

                    @Override
                    public void ensureValid(final String name, final Object value) {
                        if (Objects.isNull(value)) {
                            return;
                        }
                        assert value instanceof Long;
                        final var longValue = (Long) value;
                        if (longValue < 0) {
                            throw new ConfigException(name, value, "Value must be at least 0");
                        } else if (longValue > MAXIMUM_BACKOFF_POLICY) {
                            throw new ConfigException(name, value,
                                    "Value must be no more than " + MAXIMUM_BACKOFF_POLICY + " (24 hours)");
                        }
                    }

                    @Override
                    public String toString() {
                        return String.join(",", List.of("null", "[0, " + MAXIMUM_BACKOFF_POLICY + "]"));
                    }
                },
                ConfigDef.Importance.MEDIUM,
                "The retry backoff in milliseconds. "
                        + "This config is used to notify Kafka Connect to retry delivering a message batch or "
                        + "performing recovery in case of transient failures.",
                DELIVERY_GROUP,
                groupCounter++,
                ConfigDef.Width.NONE,
                KAFKA_RETRY_BACKOFF_MS_CONFIG
        );
        configDef.define(
            MAX_RETRIES_CONFIG,
            ConfigDef.Type.INT,
            1,
            ConfigDef.Range.atLeast(0),
            ConfigDef.Importance.MEDIUM,
            "The maximum number of times to retry on errors when sending a batch before failing the task.",
            DELIVERY_GROUP,
            groupCounter++,
            ConfigDef.Width.SHORT,
            MAX_RETRIES_CONFIG
        );
        configDef.define(
            RETRY_BACKOFF_MS_CONFIG,
            ConfigDef.Type.INT,
            3000,
            ConfigDef.Range.atLeast(0),
            ConfigDef.Importance.MEDIUM,
            "The time in milliseconds to wait following an error before a retry attempt is made.",
            DELIVERY_GROUP,
            groupCounter++,
            ConfigDef.Width.SHORT,
            RETRY_BACKOFF_MS_CONFIG
        );
    }

    @SuppressFBWarnings("DLS_DEAD_LOCAL_STORE") // Suppress groupCounter and groupCounter++
    private static void addTimeoutConfigGroup(final ConfigDef configDef) {
        int groupCounter = 0;
        configDef.define(
            HTTP_TIMEOUT_CONFIG,
            ConfigDef.Type.INT,
            30,
            ConfigDef.Range.atLeast(1),
            ConfigDef.Importance.LOW,
            "HTTP Response timeout (seconds). Default is 30 seconds.",
            TIMEOUT_GROUP,
            groupCounter++,
            ConfigDef.Width.SHORT,
            HTTP_TIMEOUT_CONFIG
        );
    }

    @SuppressFBWarnings("DLS_DEAD_LOCAL_STORE") // Suppress groupCounter and groupCounter++
    private static void addErrorsConfigGroup(final ConfigDef configDef) {
        int groupCounter = 0;
        configDef.define(
            ERRORS_TOLERANCE,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.LOW,
            "Optional errors.tolerance setting. Defaults to \"none\".",
            ERRORS_GROUP,
            groupCounter++,
            ConfigDef.Width.SHORT,
            HTTP_TIMEOUT_CONFIG

        );
    }

    public HttpSinkConfig(final Map<String, String> properties) {
        super(configDef(), properties);
        validate();
    }

    private void validate() {
        final AuthorizationType authorizationType = authorizationType();
        switch (authorizationType) {
            case STATIC:
                if (headerAuthorization() == null || headerAuthorization().isBlank()) {
                    throw new ConfigException(
                        HTTP_HEADERS_AUTHORIZATION_CONFIG,
                        getPassword(HTTP_HEADERS_AUTHORIZATION_CONFIG),
                            "Must be present when " + HTTP_HEADERS_CONTENT_TYPE_CONFIG
                            + " = " + authorizationType);
                }
                break;
            case OAUTH2:
            case APIKEY:
                if (getString(OAUTH2_ACCESS_TOKEN_URL_CONFIG) == null) {
                    throw new ConfigException(
                            OAUTH2_ACCESS_TOKEN_URL_CONFIG, getString(OAUTH2_ACCESS_TOKEN_URL_CONFIG),
                            "Must be present when " + HTTP_HEADERS_CONTENT_TYPE_CONFIG
                            + " = " + authorizationType);
                }
                if (oauth2ClientId() == null || oauth2ClientId().isEmpty()) {
                    throw new ConfigException(
                            OAUTH2_CLIENT_ID_CONFIG,
                            getString(OAUTH2_CLIENT_ID_CONFIG),
                            "Must be present when " + HTTP_HEADERS_CONTENT_TYPE_CONFIG
                            + " = " + authorizationType);
                }
                if (oauth2ClientSecret() == null || oauth2ClientSecret().value().isEmpty()) {
                    throw new ConfigException(
                            OAUTH2_CLIENT_SECRET_CONFIG,
                            getPassword(OAUTH2_CLIENT_SECRET_CONFIG),
                            "Must be present when " + HTTP_HEADERS_CONTENT_TYPE_CONFIG
                            + " = " + authorizationType);
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

        // don't let configuration have both errors.tolerance=all and batching.enabled=true
        if (batchingEnabled() && errorsTolerance().equalsIgnoreCase("all")) {
            throw new ConfigException("Cannot use errors.tolerance when batching is enabled");
        }

    }

    public final URI httpUri() {
        return toURI(HTTP_URL_CONFIG);
    }

    public final Long kafkaRetryBackoffMs() {
        return getLong(KAFKA_RETRY_BACKOFF_MS_CONFIG);
    }

    public Map<String, String> getAdditionalHeaders() {
        return getList(HTTP_HEADERS_ADDITIONAL).stream()
                .map(s -> s.split(HTTP_HEADERS_ADDITIONAL_DELIMITER))
                .collect(Collectors.toMap(h -> h[0], h -> h[1]));
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

    public final boolean batchingEnabled() {
        return getBoolean(BATCHING_ENABLED_CONFIG);
    }

    public final int batchMaxSize() {
        return getInt(BATCH_MAX_SIZE_CONFIG);
    }

    // currently just getting this for a configuration check
    private final String errorsTolerance() {
        return getString(ERRORS_TOLERANCE) != null ? getString(ERRORS_TOLERANCE) : "";
    }

    // White space is significant for our batch delimiters but ConfigKey trims it out
    // so we need to check the originals rather than using the normal machinery.
    private String getOriginalString(final String key, final String defaultValue) {
        get(key); // Assess key via the normal flow so it isn't reported as "unused".
        return originalsStrings().getOrDefault(key, defaultValue);
    }

    public final String batchPrefix() {
        return getOriginalString(BATCH_PREFIX_CONFIG, BATCH_PREFIX_DEFAULT);
    }

    public final String batchSuffix() {
        return getOriginalString(BATCH_SUFFIX_CONFIG, BATCH_SUFFIX_DEFAULT);
    }

    public final String batchSeparator() {
        return getOriginalString(BATCH_SEPARATOR_CONFIG, BATCH_SEPARATOR_DEFAULT);
    }

    public int maxRetries() {
        return getInt(MAX_RETRIES_CONFIG);
    }

    public int retryBackoffMs() {
        return getInt(RETRY_BACKOFF_MS_CONFIG);
    }

    public int httpTimeout() {
        return getInt(HTTP_TIMEOUT_CONFIG);
    }

    public final String connectorName() {
        return originalsStrings().get(NAME_CONFIG);
    }

    public final URI oauth2AccessTokenUri() {
        return toURI(OAUTH2_ACCESS_TOKEN_URL_CONFIG);
    }

    private URI toURI(final String propertyName) {
        try {
            return new URL(getString(propertyName)).toURI();
        } catch (final MalformedURLException | URISyntaxException e) {
            throw new ConnectException(String.format("Could not retrieve proper URI from %s", propertyName), e);
        }
    }

    public final String oauth2ClientId() {
        return getString(OAUTH2_CLIENT_ID_CONFIG);
    }

    public final Password oauth2ClientSecret() {
        return getPassword(OAUTH2_CLIENT_SECRET_CONFIG);
    }

    public final OAuth2AuthorizationMode oauth2AuthorizationMode() {
        return OAuth2AuthorizationMode.valueOf(getString(OAUTH2_CLIENT_AUTHORIZATION_MODE_CONFIG).toUpperCase());
    }

    public final String oauth2ClientScope() {
        return getString(OAUTH2_CLIENT_SCOPE_CONFIG);
    }

    public final String oauth2ResponseTokenProperty() {
        return getString(OAUTH2_RESPONSE_TOKEN_PROPERTY_CONFIG);
    }

    public static void main(final String... args) {
        System.out.println("=========================================");
        System.out.println("HTTP Sink connector Configuration Options");
        System.out.println("=========================================");
        System.out.println();
        System.out.println(configDef().toEnrichedRst());
    }

}
