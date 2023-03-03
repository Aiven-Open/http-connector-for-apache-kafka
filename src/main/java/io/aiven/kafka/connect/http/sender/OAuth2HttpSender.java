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
import java.net.http.HttpResponse;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;
import io.aiven.kafka.connect.http.sender.HttpRequestBuilder.OAuth2HttpRequestBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class OAuth2HttpSender extends HttpSender {

    private final Logger logger = LoggerFactory.getLogger(OAuth2HttpSender.class);

    private String accessTokenAuthHeader;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final OAuth2HttpRequestBuilder oauth2HttpRequestBuilder;

    OAuth2HttpSender(final HttpSinkConfig config, final OAuth2HttpRequestBuilder accessTokenRequestBuilder) {
        super(config);
        this.oauth2HttpRequestBuilder = accessTokenRequestBuilder;
    }

    //for testing
    OAuth2HttpSender(final HttpSinkConfig config, final HttpClient httpClient) {
        super(config, httpClient);
        this.oauth2HttpRequestBuilder = new AccessTokenHttpRequestBuilder();
    }

    @Override
    protected HttpResponse<String> sendWithRetries(final HttpRequest.Builder requestBuilder,
                                                   final HttpResponseHandler originHttpResponseHandler) {
        final var accessTokenAwareRequestBuilder = loadOrRefreshAccessToken(requestBuilder);
        final HttpResponseHandler handler = response -> {
            if (response.statusCode() == 401) { // access denied or refresh of a token is needed
                this.accessTokenAuthHeader = null;
                this.sendWithRetries(requestBuilder, originHttpResponseHandler);
            } else {
                originHttpResponseHandler.onResponse(response);
            }
        };
        return super.sendWithRetries(accessTokenAwareRequestBuilder, handler);
    }

    private HttpRequest.Builder loadOrRefreshAccessToken(final HttpRequest.Builder requestBuilder) {
        if (accessTokenAuthHeader == null) {
            logger.info("Configure OAuth2 for URI: {} and Client ID: {}",
                    config.oauth2AccessTokenUri(), config.oauth2ClientId());
            try {
                final var response =
                        super.sendWithRetries(
                                oauth2HttpRequestBuilder.build(config),
                                HttpResponseHandler.ON_HTTP_ERROR_RESPONSE_HANDLER
                        );
                accessTokenAuthHeader = buildAccessTokenAuthHeader(response.body());
            } catch (final IOException e) {
                throw new ConnectException("Couldn't get OAuth2 access token", e);
            }
        }
        return requestBuilder.setHeader(HttpRequestBuilder.HEADER_AUTHORIZATION, accessTokenAuthHeader);
    }

    private String buildAccessTokenAuthHeader(final String responseBody) throws JsonProcessingException {
        final var accessTokenResponse =
                objectMapper.readValue(responseBody, new TypeReference<Map<String, String>>() {});
        if (!accessTokenResponse.containsKey(config.oauth2ResponseTokenProperty())) {
            throw new ConnectException(
                    "Couldn't find access token property "
                            + config.oauth2ResponseTokenProperty()
                            + " in response properties: "
                            + accessTokenResponse.keySet());
        }
        final var tokenType = accessTokenResponse.getOrDefault("token_type", "Bearer");
        final var accessToken = accessTokenResponse.get(config.oauth2ResponseTokenProperty());
        return String.format("%s %s", tokenType, accessToken);
    }

}
