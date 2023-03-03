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

import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.StringJoiner;

import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.kafka.connect.http.config.AuthorizationType;
import io.aiven.kafka.connect.http.config.HttpSinkConfig;
import io.aiven.kafka.connect.http.sender.HttpRequestBuilder.OAuth2HttpRequestBuilder;

class AccessTokenHttpRequestBuilder implements OAuth2HttpRequestBuilder {

    AccessTokenHttpRequestBuilder() {
    }

    @Override
    public HttpRequest.Builder build(final HttpSinkConfig config) {
        VALIDATE.accept(config, AuthorizationType.OAUTH2);

        final var accessTokenRequestBuilder = HttpRequest
                .newBuilder(config.oauth2AccessTokenUri())
                .timeout(Duration.ofSeconds(config.httpTimeout()))
                .header(HEADER_CONTENT_TYPE, HEADER_CONTENT_TYPE_FORM);

        final var accessTokenRequestBodyBuilder = new StringJoiner("&");
        accessTokenRequestBodyBuilder.add(encodeNameAndValue("grant_type", "client_credentials"));
        if (config.oauth2ClientScope() != null) {
            accessTokenRequestBodyBuilder.add(encodeNameAndValue("scope", config.oauth2ClientScope()));
        }

        setClientIdAndSecret(accessTokenRequestBuilder, accessTokenRequestBodyBuilder, config);
        return accessTokenRequestBuilder
                .POST(HttpRequest.BodyPublishers.ofString(accessTokenRequestBodyBuilder.toString()));
    }

    private void setClientIdAndSecret(final HttpRequest.Builder httpRequestBuilder,
                                      final StringJoiner requestBodyBuilder,
                                      final HttpSinkConfig config) {
        switch (config.oauth2AuthorizationMode()) {
            case HEADER:
                addClientIdAndSecretInRequestHeader(httpRequestBuilder, config);
                break;
            case URL:
                addClientIdAndSecretInRequestBody(requestBodyBuilder, config);
                break;
            default:
                throw new ConnectException("Unknown OAuth2 authorization mode: " + config.oauth2AuthorizationMode());
        }
    }

    private void addClientIdAndSecretInRequestHeader(final HttpRequest.Builder httpRequestBuilder,
                                                     final HttpSinkConfig config) {
        final var clientAndSecretBytes =
                (config.oauth2ClientId() + ":" + config.oauth2ClientSecret().value())
                        .getBytes(StandardCharsets.UTF_8);
        final var clientAndSecretAuthHeader =
                "Basic " + Base64.getEncoder().encodeToString(clientAndSecretBytes);
        httpRequestBuilder.header(HEADER_AUTHORIZATION, clientAndSecretAuthHeader);
    }

    private void addClientIdAndSecretInRequestBody(final StringJoiner requestBodyBuilder,
                                                   final HttpSinkConfig config) {
        requestBodyBuilder
                .add(encodeNameAndValue("client_id", config.oauth2ClientId()))
                .add(encodeNameAndValue("client_secret", config.oauth2ClientSecret().value()));
    }

    private String encodeNameAndValue(final String name, final String value) {
        return String.format("%s=%s", encode(name), encode(value));
    }

    private String encode(final String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

}
