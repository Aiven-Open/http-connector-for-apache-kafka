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
        final HttpSinkConfig config, final HttpClient httpClient, final AccessTokenHttpSender accessTokenHttpSender
    ) {
        super(config, new OAuth2AuthHttpRequestBuilder(config, accessTokenHttpSender), httpClient);
    }

    @Override
    protected HttpResponse<String> sendWithRetries(
        final Builder requestBuilderWithPayload,
        final HttpResponseHandler originHttpResponseHandler,
        final int retriesNumber
    ) {
        final HttpResponseHandler handler = (response, retriesRemain) -> {
            if (response.statusCode() == 401 && retriesRemain > 0) { // access denied or refresh of a token is needed
                ((OAuth2AuthHttpRequestBuilder) this.httpRequestBuilder).renewAccessToken(requestBuilderWithPayload);
                this.sendWithRetries(requestBuilderWithPayload, originHttpResponseHandler, retriesRemain - 1);
            } else {
                originHttpResponseHandler.onResponse(response, retriesRemain);
            }
        };
        return super.sendWithRetries(requestBuilderWithPayload, handler, retriesNumber);
    }

    static class OAuth2AuthHttpRequestBuilder extends DefaultHttpRequestBuilder {

        private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2AuthHttpRequestBuilder.class);
        private final ObjectMapper objectMapper = new ObjectMapper();
        private String accessTokenAuthHeader;
        private final AccessTokenHttpSender accessTokenHttpSender;
        private final HttpSinkConfig config;

        OAuth2AuthHttpRequestBuilder(final HttpSinkConfig config, final AccessTokenHttpSender accessTokenHttpSender) {
            this.config = config;
            this.accessTokenHttpSender = accessTokenHttpSender;
        }

        @Override
        public Builder build(final HttpSinkConfig config) {
            return super
                .build(config)
                .header(HEADER_AUTHORIZATION, getAccessToken());
        }

        void renewAccessToken(final HttpRequest.Builder requestBuilderWithPayload) {
            this.accessTokenAuthHeader = null;
            requestBuilderWithPayload.setHeader(HttpRequestBuilder.HEADER_AUTHORIZATION, this.getAccessToken());
        }

        private String getAccessToken() {
            if (this.accessTokenAuthHeader != null) {
                return this.accessTokenAuthHeader;
            }
            LOGGER.info("Configure OAuth2 for URI: {} and Client ID: {}", config.oauth2AccessTokenUri(),
                config.oauth2ClientId());
            try {
                final var response = accessTokenHttpSender.call();
                accessTokenAuthHeader = buildAccessTokenAuthHeader(response.body());
            } catch (final IOException e) {
                throw new ConnectException("Couldn't get OAuth2 access token", e);
            }
            return accessTokenAuthHeader;
        }

        private String buildAccessTokenAuthHeader(final String oauth2ResponseBody) throws JsonProcessingException {
            final var accessTokenResponse =
                objectMapper.readValue(oauth2ResponseBody, new TypeReference<Map<String, String>>() {});
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
