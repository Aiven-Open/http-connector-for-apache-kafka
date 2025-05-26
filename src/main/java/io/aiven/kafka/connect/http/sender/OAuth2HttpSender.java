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

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;
import io.aiven.kafka.connect.http.sender.DefaultHttpSender.DefaultHttpRequestBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OAuth2HttpSender extends AbstractHttpSender implements HttpSender {

    OAuth2HttpSender(
        final HttpSinkConfig config,
        final HttpClient httpClient,
        final OAuth2AccessTokenHttpSender oauth2AccessTokenHttpSender
    ) {
        super(config, new OAuth2AuthHttpRequestBuilder(config, oauth2AccessTokenHttpSender), httpClient);
    }

    @Override
    protected HttpResponse<String> sendWithRetries(
        final Builder requestBuilder, final HttpResponseHandler originHttpResponseHandler, final int retries
    ) {
        // This handler allows to request a new access token if a an expected error occurs, meaning the session might be expired
        final HttpResponseHandler handler = (response, remainingRetries) -> {
            // If the response has one of the configured error codes and we have retries left, we attempt to renew the session
            if (config.oauth2RenewTokenOnStatusCodes().contains(response.statusCode()) && remainingRetries > 0) {
                // Update the request builder with the new access token
                ((OAuth2AuthHttpRequestBuilder) this.httpRequestBuilder).renewAccessToken(requestBuilder);
                // Retry the call and decrease the retries counter to avoid looping on token renewal
                this.sendWithRetries(requestBuilder, originHttpResponseHandler, remainingRetries - 1);
            } else {
                originHttpResponseHandler.onResponse(response, remainingRetries);
            }
        };
        return super.sendWithRetries(requestBuilder, handler, retries);
    }

    static class OAuth2AuthHttpRequestBuilder extends DefaultHttpRequestBuilder {

        private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2AuthHttpRequestBuilder.class);
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        private final HttpSinkConfig config;
        private final OAuth2AccessTokenHttpSender oauth2AccessTokenHttpSender;

        private String accessToken;

        OAuth2AuthHttpRequestBuilder(
            final HttpSinkConfig config,
            final OAuth2AccessTokenHttpSender oauth2AccessTokenHttpSender
        ) {
            this.config = config;
            this.oauth2AccessTokenHttpSender = oauth2AccessTokenHttpSender;
        }

        @Override
        public Builder build(final HttpSinkConfig config) {
            return super.build(config)
                        // We need to retrieve an access token first
                        .header(HEADER_AUTHORIZATION, requestAccessToken());
        }

        /**
         * When expired, reinitialize the current token, request a new one and update the request builder
         *
         * @param requestBuilder the request builder used to call the protected URI
         */
        void renewAccessToken(final HttpRequest.Builder requestBuilder) {
            this.accessToken = null;
            requestBuilder.setHeader(HttpRequestBuilder.HEADER_AUTHORIZATION, this.requestAccessToken());
        }

        /**
         * Retrieves the current access token or requests it if none is defined
         *
         * @return an access token
         */
        private String requestAccessToken() {
            // Re-use the access token if it's already defined
            if (this.accessToken != null) {
                return this.accessToken;
            }
            LOGGER.info("Configure OAuth2 for URI: {} and Client ID: {}", config.oauth2AccessTokenUri(),
                config.oauth2ClientId());
            try {
                // Whenever the access token is null (not initialized yet or expired), call the AccessTokenHttpSender
                // implementation to request one
                final var response = oauth2AccessTokenHttpSender.call();
                accessToken = buildAccessTokenAuthHeader(response.body());
            } catch (final IOException e) {
                throw new ConnectException("Couldn't get OAuth2 access token", e);
            }
            return accessToken;
        }

        private String buildAccessTokenAuthHeader(final String oauth2ResponseBody) throws JsonProcessingException {
            final var accessTokenResponse =
                OBJECT_MAPPER.readValue(oauth2ResponseBody, new TypeReference<Map<String, String>>() {});
            if (!accessTokenResponse.containsKey(config.oauth2ResponseTokenProperty())) {
                throw new ConnectException("Couldn't find access token property " + config.oauth2ResponseTokenProperty()
                                           + " in response properties: " + accessTokenResponse.keySet());
            }
            final var tokenType = accessTokenResponse.getOrDefault("token_type", "Bearer");
            final var accessToken = accessTokenResponse.get(config.oauth2ResponseTokenProperty());
            return String.format("%s %s", tokenType, accessToken);
        }

    }

}
