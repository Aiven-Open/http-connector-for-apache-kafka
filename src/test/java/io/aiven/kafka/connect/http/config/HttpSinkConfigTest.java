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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.from;

final class HttpSinkConfigTest {

    @Test
    void requiredConfigurations() {
        final Map<String, String> properties = Map.of();
        assertThatExceptionOfType(ConfigException.class)
                .describedAs("Expected config exception due to missing http.url")
                .isThrownBy(() -> new HttpSinkConfig(properties))
                .withMessage("Missing required configuration \"http.url\" which has no default value.");

    }

    @Test
    void correctMinimalConfig() throws URISyntaxException {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none"
        );

        final HttpSinkConfig config = new HttpSinkConfig(properties);
        assertThat(config)
                .returns(new URI("http://localhost:8090"), from(HttpSinkConfig::httpUri))
                .returns(AuthorizationType.NONE, from(HttpSinkConfig::authorizationType))
                .returns(null, from(HttpSinkConfig::headerContentType))
                .returns(false, from(HttpSinkConfig::batchingEnabled))
                .returns(500, from(HttpSinkConfig::batchMaxSize))
                .returns(1, from(HttpSinkConfig::maxRetries))
                .returns(3000, from(HttpSinkConfig::retryBackoffMs))
                .returns(Collections.emptyMap(), from(HttpSinkConfig::getAdditionalHeaders))
                .returns(null, from(HttpSinkConfig::oauth2ClientId))
                .returns(null, from(HttpSinkConfig::oauth2ClientSecret))
                .returns(null, from(HttpSinkConfig::oauth2ClientScope))
                .returns(OAuth2AuthorizationMode.HEADER, from(HttpSinkConfig::oauth2AuthorizationMode))
                .returns("access_token", from(HttpSinkConfig::oauth2ResponseTokenProperty))
                .returns(null, from(HttpSinkConfig::kafkaRetryBackoffMs));
    }

    @Test
    void invalidOAuth2AccessTokenUrl() {
        final var emptyAccessTokenUrlConfig = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "oauth2.access.token.url", ""
        );

        assertThatExceptionOfType(ConfigException.class)
                .describedAs("Expected config exception due to empty OAuth access token URL")
                .isThrownBy(() -> new HttpSinkConfig(emptyAccessTokenUrlConfig))
                .withMessage("Invalid value  for configuration oauth2.access.token.url: malformed URL");

        final var wrongAccessTokenUrlConfig = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "oauth2.access.token.url", ";http://localhost:8090"
        );

        assertThatExceptionOfType(ConfigException.class)
                .describedAs("Expected config exception due to malformed OAuth access token URL")
                .isThrownBy(() -> new HttpSinkConfig(wrongAccessTokenUrlConfig))
                .withMessage("Invalid value ;http://localhost:8090 for configuration oauth2.access.token.url: malformed URL");
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

        assertThatExceptionOfType(ConfigException.class)
                .describedAs("Expected config exception due to invalid client authorization mode")
                .isThrownBy(() -> new HttpSinkConfig(invalidClientAuthorizationModeConfig))
                .withMessage("Invalid value AAAABBBCCCC for configuration oauth2.client.authorization.mode: "
                        + "supported values are: [HEADER, URL]");
    }


    @ParameterizedTest
    @ValueSource(strings = {"OAUTH2", "APIKEY"})
    void invalidOAuth2Configuration(final String input) {
        final var noAccessTokenUrlConfig = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", input.toLowerCase()
        );

        assertThatExceptionOfType(ConfigException.class)
                .describedAs("Expected config exception due to missing OAuth access token URL")
                .isThrownBy(() -> new HttpSinkConfig(noAccessTokenUrlConfig))
                .withMessage("Invalid value null for configuration oauth2.access.token.url: "
                        + "Must be present when http.headers.content.type = " + input);

        final var noSecretIdConfig = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", input.toLowerCase(),
                "oauth2.access.token.url", "http://localhost:8090/token"
        );

        assertThatExceptionOfType(ConfigException.class)
                .describedAs("Expected config exception due to missing OAuth client id")
                .isThrownBy(() -> new HttpSinkConfig(noSecretIdConfig))
                .withMessage("Invalid value null for configuration oauth2.client.id: "
                        + "Must be present when http.headers.content.type = " + input);

        final var noSecretConfig = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", input.toLowerCase(),
                "oauth2.access.token.url", "http://localhost:8090/token",
                "oauth2.client.id", "client_id"
        );

        assertThatExceptionOfType(ConfigException.class)
                .describedAs("Expected config exception due to missing OAuth client secret")
                .isThrownBy(() -> new HttpSinkConfig(noSecretConfig))
                .withMessage("Invalid value null for configuration oauth2.client.secret: "
                        + "Must be present when http.headers.content.type = " + input);
    }

    @Test
    void validOAuth2MinimalConfiguration() throws URISyntaxException {

        final var oauth2Config = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "oauth2",
                "oauth2.access.token.url", "http://localhost:8090/token",
                "oauth2.client.id", "client_id",
                "oauth2.client.secret", "client_secret"
        );

        final var config = new HttpSinkConfig(oauth2Config);

        assertThat(config)
                .returns(new URI("http://localhost:8090/token"), from(HttpSinkConfig::oauth2AccessTokenUri))
                .returns("client_id", from(HttpSinkConfig::oauth2ClientId))
                .returns("client_secret", from(httpSinkConfig -> httpSinkConfig.oauth2ClientSecret().value()))
                .returns(null, from(HttpSinkConfig::oauth2ClientScope))
                .returns(OAuth2AuthorizationMode.HEADER, from(HttpSinkConfig::oauth2AuthorizationMode))
                .returns("access_token", from(HttpSinkConfig::oauth2ResponseTokenProperty));
    }

    @Test
    void validOAuth2FullConfiguration() throws URISyntaxException {

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

        assertThat(config)
                .returns(new URI("http://localhost:8090/token"), from(HttpSinkConfig::oauth2AccessTokenUri))
                .returns("client_id", from(HttpSinkConfig::oauth2ClientId))
                .returns("client_secret", from(httpSinkConfig -> httpSinkConfig.oauth2ClientSecret().value()))
                .returns("scope1,scope2", from(HttpSinkConfig::oauth2ClientScope))
                .returns(OAuth2AuthorizationMode.URL, from(HttpSinkConfig::oauth2AuthorizationMode))
                .returns("moooooo", from(HttpSinkConfig::oauth2ResponseTokenProperty));
    }

    @Test
    void invalidUrl() {
        final Map<String, String> properties = Map.of(
                "http.url", "#http://localhost:8090",
                "http.authorization.type", "none"
        );

        assertThatExceptionOfType(ConfigException.class)
                .describedAs("Expected config exception due to malformed http.url")
                .isThrownBy(() -> new HttpSinkConfig(properties))
                .withMessage("Invalid value #http://localhost:8090 for configuration http.url: malformed URL");
    }

    @Test
    void missingAuthorizationType() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090"
        );


        assertThatExceptionOfType(ConfigException.class)
                .describedAs("Expected config exception due to missing authorization type")
                .isThrownBy(() -> new HttpSinkConfig(properties))
                .withMessage("Missing required configuration \"http.authorization.type\" which has no default value.");
    }

    @ParameterizedTest
    @MethodSource("authProperties")
    void supportedAuthorizationType(
            final AuthorizationType expectedAuthorizationType,
            final Map<String, String> properties
    ) {
        final HttpSinkConfig config = new HttpSinkConfig(properties);

        assertThat(config.authorizationType()).isEqualTo(expectedAuthorizationType);
    }

    private static Stream<Arguments> authProperties() {
        return Stream.of(
                Arguments.of(AuthorizationType.NONE,
                        Map.of(
                                "http.url", "http://localhost:8090",
                                "http.authorization.type", AuthorizationType.NONE.name
                        )),
                Arguments.of(AuthorizationType.STATIC,
                        Map.of(
                                "http.url", "http://localhost:8090",
                                "http.authorization.type", AuthorizationType.STATIC.name,
                                "http.headers.authorization", "some"
                        )),
                Arguments.of(AuthorizationType.OAUTH2,
                        Map.of(
                                "http.url", "http://localhost:8090",
                                "http.authorization.type", AuthorizationType.OAUTH2.name,
                                "oauth2.access.token.url", "http://localhost:42",
                                "oauth2.client.id", "client_id",
                                "oauth2.client.secret", "client_secret"
                        )),
                Arguments.of(AuthorizationType.APIKEY,
                        Map.of(
                                "http.url", "http://localhost:8090",
                                "http.authorization.type", AuthorizationType.APIKEY.name,
                                "oauth2.access.token.url", "http://localhost:42",
                                "oauth2.client.id", "key",
                                "oauth2.client.secret", "secret"
                        ))
        );
    }

    @Test
    void unsupportedAuthorizationType() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "unsupported"
        );

        assertThatExceptionOfType(ConfigException.class)
                .describedAs("Expected config exception due to unsupported authorization type")
                .isThrownBy(() -> new HttpSinkConfig(properties))
                .withMessage("Invalid value unsupported for configuration http.authorization.type: "
                        + "supported values are: [none, oauth2, static, apikey]");
    }

    @Test
    void recommendedValuesForAuthorizationType() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090"
        );

        assertThat(HttpSinkConfig.configDef().validate(properties))
                .filteredOn(x -> x.name().equals("http.authorization.type"))
                .first().extracting(ConfigValue::recommendedValues).asList()
                .containsExactlyElementsOf(AuthorizationType.NAMES);
    }

    @Test
    void missingAuthorizationHeaderWhenAuthorizationTypeStatic() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "static"
        );

        assertThatExceptionOfType(ConfigException.class)
                .describedAs("Expected config exception due to missing authorization headers")
                .isThrownBy(() -> new HttpSinkConfig(properties))
                .withMessage("Invalid value null for configuration http.headers.authorization: "
                        + "Must be present when http.headers.content.type = STATIC");
    }

    @Test
    void presentAuthorizationHeaderWhenAuthorizationTypeNotStatic() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "http.headers.authorization", "some"
        );

        assertThatExceptionOfType(ConfigException.class)
                .describedAs("Expected config exception due to invalid authorization header")
                .isThrownBy(() -> new HttpSinkConfig(properties))
                .withMessage("Invalid value [hidden] for configuration http.headers.authorization: "
                        + "Must not be present when http.headers.content.type != STATIC");
    }

    @Test
    void headerContentType() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "http.headers.content.type", "application/json"
        );

        final HttpSinkConfig config = new HttpSinkConfig(properties);
        assertThat(config.headerContentType()).isEqualTo("application/json");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "        ", " \r\n       "})
    void checkContentTypeValidationWithBlankValues(final String contentType) {
        final Map<String, String> properties = new HashMap<>(Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "http.headers.content.type", contentType
        ));
        assertThatExceptionOfType(ConfigException.class)
                .describedAs("Expected config exception due to empty or blank content type")
                .isThrownBy(() -> new HttpSinkConfig(properties));
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

        assertThat(additionalHeaders)
            .containsOnly(
                entry("test", "value"),
                entry("test2", "value2"),
                entry("test4", "value4")
            );
    }

    @ParameterizedTest
    @CsvSource({
        "'test:value,test:valueUpperCase',   Duplicate keys are not allowed (case-insensitive)",
        "'test:value,TEST:valueUpperCase',   Duplicate keys are not allowed (case-insensitive)",
        "'test:value,wrong,test1:test1',     Header field should use format header:value",
        "'test:value,,test2:test2',          Header field should use format header:value"
    })
    void checkAdditionalHeadersValidation(final String headersValue, final String expectedExceptionMessage) {
        final Map<String, String> properties = new HashMap<>(Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "http.headers.additional", headersValue
        ));

        assertThatExceptionOfType(ConfigException.class)
                .isThrownBy(() -> new HttpSinkConfig(properties))
                .withMessage(expectedExceptionMessage);
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
        assertThat(config.batchingEnabled()).isTrue();
        assertThat(config.batchMaxSize()).isEqualTo(123456);
    }

    @Test
    void tooBigBatchSize() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "batching.enabled", "true",
                "batch.max.size", "1000001"
        );

        assertThatExceptionOfType(ConfigException.class)
                .describedAs("Expected config exception due to invalid maximum batch size")
                .isThrownBy(() -> new HttpSinkConfig(properties))
                .withMessage("Invalid value 1000001 for configuration batch.max.size: "
                        + "Value must be no more than 1000000");
    }

    @Test
    void negativeMaxRetries() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "max.retries", "-1"
        );

        assertThatExceptionOfType(ConfigException.class)
                .describedAs("Expected config exception due to invalid maximum number of retries")
                .isThrownBy(() -> new HttpSinkConfig(properties))
                .withMessage("Invalid value -1 for configuration max.retries: Value must be at least 0");
    }

    @Test
    void correctMaxRetries() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "max.retries", "123"
        );

        final HttpSinkConfig config = new HttpSinkConfig(properties);
        assertThat(config.maxRetries()).isEqualTo(123);
    }

    @Test
    void negativeRetryBackoffMs() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "retry.backoff.ms", "-1"
        );

        assertThatExceptionOfType(ConfigException.class)
                .describedAs("Expected config exception due to invalid value of retry.backoff.ms")
                .isThrownBy(() -> new HttpSinkConfig(properties))
                .withMessage("Invalid value -1 for configuration retry.backoff.ms: Value must be at least 0");
    }

    @Test
    void tooBigKafkaRetryBackoffMs() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "kafka.retry.backoff.ms", String.valueOf(TimeUnit.HOURS.toMillis(25))
        );

        assertThatExceptionOfType(ConfigException.class)
                .describedAs("Expected config exception due to invalid value of kafka.retry.backoff.ms")
                .isThrownBy(() -> new HttpSinkConfig(properties))
                .withMessage("Invalid value 90000000 for configuration kafka.retry.backoff.ms: "
                        + "Value must be no more than 86400000 (24 hours)");
    }

    @Test
    void negativeKafkaRetryBackoffMs() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "kafka.retry.backoff.ms", "-1"
        );

        assertThatExceptionOfType(ConfigException.class)
                .describedAs("Expected config exception due to invalid value of kafka.retry.backoff.ms")
                .isThrownBy(() -> new HttpSinkConfig(properties))
                .withMessage("Invalid value -1 for configuration kafka.retry.backoff.ms: Value must be at least 0");
    }

    @Test
    void correctRetryBackoffMs() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "retry.backoff.ms", "12345"
        );

        final HttpSinkConfig config = new HttpSinkConfig(properties);
        assertThat(config.retryBackoffMs()).isEqualTo(12345);
    }

    @Test
    void customKafkaRetryBackoffMs() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "kafka.retry.backoff.ms", "6000"
        );

        final var config = new HttpSinkConfig(properties);
        assertThat(config.kafkaRetryBackoffMs()).isEqualTo(6000);
    }

    @Test
    void defaultHttpTimeout() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none"
        );

        final var config = new HttpSinkConfig(properties);
        assertThat(config.httpTimeout()).isEqualTo(30);
    }

    @Test
    void customHttpTimeout() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "http.timeout", "5"
        );

        final var config = new HttpSinkConfig(properties);
        assertThat(config.httpTimeout()).isEqualTo(5);
    }

    @Test
    void invalidConfig() {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "batching.enabled", "true",
                "errors.tolerance", "all"
        );

        assertThatExceptionOfType(ConfigException.class)
                .describedAs("Cannot use errors.tolerance when batching is enabled")
                .isThrownBy(() -> new HttpSinkConfig(properties))
                .withMessage("Cannot use errors.tolerance when batching is enabled");
    }

    @Test
    void validBatchingConfig() {
        Map<String, String> properties;

        properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "batching.enabled", "false",
                "errors.tolerance", "all"
        );

        var config = new HttpSinkConfig(properties);
        assertThat(config.batchingEnabled()).isFalse();

        properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "batching.enabled", "true"
        );

        config = new HttpSinkConfig(properties);
        assertThat(config.batchingEnabled()).isTrue();

        properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none",
                "batching.enabled", "true",
                "errors.tolerance", "none"
        );

        config = new HttpSinkConfig(properties);
        assertThat(config.batchingEnabled()).isTrue();
    }
}
