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

import java.net.http.HttpRequest;
import java.time.Duration;

import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.kafka.connect.http.config.AuthorizationType;
import io.aiven.kafka.connect.http.config.HttpSinkConfig;
import io.aiven.kafka.connect.http.sender.HttpRequestBuilder.OAuth2HttpRequestBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ApiKeyAccessTokenHttpRequestBuilder implements OAuth2HttpRequestBuilder {
    ApiKeyAccessTokenHttpRequestBuilder() {
    }

    @Override
    public HttpRequest.Builder build(final HttpSinkConfig config) {
        VALIDATE.accept(config, AuthorizationType.APIKEY);

        final var accessTokenRequestBuilder = HttpRequest
                .newBuilder(config.oauth2AccessTokenUri())
                .timeout(Duration.ofSeconds(config.httpTimeout()))
                .header(HEADER_CONTENT_TYPE, HEADER_CONTENT_TYPE_FORM);

        final AccessTokenRequest accessTokenRequest = new AccessTokenRequest("api-key", config.oauth2ClientId(),
                config.oauth2ClientSecret().value());

        final ObjectMapper objectMapper = new ObjectMapper();
        try {
            final String requestBody = objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(accessTokenRequest);
            return accessTokenRequestBuilder
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody));
        } catch (final JsonProcessingException e) {
            throw new ConnectException("Cannot get OAuth2 access token from ApiKey Authentication", e);
        }
    }

    private static class AccessTokenRequest {
        final String type;
        final String key;
        final String secret;

        public AccessTokenRequest(final String type, final String key, final String secret) {
            this.type = type;
            this.key = key;
            this.secret = secret;
        }

        //Getters needed for serdes
        public String getType() {
            return type;
        }

        public String getKey() {
            return key;
        }

        public String getSecret() {
            return secret;
        }
    }

}
