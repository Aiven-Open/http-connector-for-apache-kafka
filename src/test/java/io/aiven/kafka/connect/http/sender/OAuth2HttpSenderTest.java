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
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OAuth2HttpSenderTest extends HttpSenderTestUtils<OAuth2HttpSender> {

    static final String ACCESS_TOKEN_RESPONSE =
        "{\"access_token\": \"my_access_token\",\"token_type\": \"Bearer\",\"expires_in\": 7199}";
    @Mock
    private AccessTokenHttpSender accessTokenHttpSender;

    @Override
    protected OAuth2HttpSender buildHttpSender(final HttpSinkConfig config, final HttpClient client) {
        return new OAuth2HttpSender(config, client, accessTokenHttpSender);
    }

    @Test
    void shouldBuildDefaultHttpRequest() throws Exception {
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(ACCESS_TOKEN_RESPONSE);
        when(accessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse);

        super.assertHttpSender(
            defaultConfig(), List.of("some message"), httpRequests -> httpRequests.forEach(httpRequest -> assertThat(
                httpRequest
                    .headers()
                    .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)
                    .orElse(null)).isEqualTo("Bearer my_access_token")));
    }

    @Test
    void buildAccessTokenAuthHeaderFromCustomSettings() throws IOException, InterruptedException {

        final String basicTokenResponse = "{\"some_token\": \"my_basic_token\",\"token_type\": \"Basic\"}";

        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(basicTokenResponse);
        when(accessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse);

        final var config = new HashMap<>(defaultConfig());
        config.put("oauth2.response.token.property", "some_token");

        super.assertHttpSender(
            config, List.of("some message"), httpRequests -> httpRequests.forEach(httpRequest -> assertThat(httpRequest
                .headers()
                .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)
                .orElse(null)).isEqualTo("Basic " + "my_basic_token")));

    }

    @Test
    void reuseAccessToken() throws Exception {
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(ACCESS_TOKEN_RESPONSE);
        when(accessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse);

        super.assertHttpSender(defaultConfig(), List.of("some message 1", "some message 2"),
            httpRequests -> httpRequests.forEach(httpRequest -> assertThat(httpRequest
                .headers()
                .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)
                .orElse(null)).isEqualTo("Bearer my_access_token")));

        verify(accessTokenHttpSender, times(1)).call();

    }

    @Test
    void refreshAccessToken() throws Exception {

        // first call to retrieve an access token
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(ACCESS_TOKEN_RESPONSE);
        // second call to retrieve an access token
        final HttpResponse<String> mockedAccessTokenResponseRefreshed = mock(HttpResponse.class);
        when(mockedAccessTokenResponseRefreshed.body()).thenReturn(
            "{\"access_token\": \"my_refreshed_token\",\"token_type\": \"Bearer\",\"expires_in\": 7199}");
        when(accessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse, mockedAccessTokenResponseRefreshed);

        // Mock a 2nd response with 401.
        final HttpResponse<String> errorResponse = mock(HttpResponse.class);
        when(errorResponse.statusCode()).thenReturn(401);
        // Mock a 2nd response with 401.
        final HttpResponse<String> normalResponse = mock(HttpResponse.class);
        when(normalResponse.statusCode()).thenReturn(200);

        super.assertHttpSender(defaultConfig(), List.of("some message 1", "some message 2"), httpRequests -> {
            // 3 attempts were made
            assertThat(httpRequests.size()).isEqualTo(3);
            IntStream
                .of(httpRequests.size())
                .boxed()
                .forEach(httpRequestIndex -> {
                    // First time the access token is my_access_token
                    if (httpRequestIndex == 0) {
                        assertThat(httpRequests
                            .get(0)
                            .headers()
                            .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)
                            .orElse(null)).isEqualTo("Bearer my_access_token");
                    } else {
                        // Every other calls are with my_refreshed_token
                        assertThat(httpRequests
                            .get(httpRequestIndex - 1)
                            .headers()
                            .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)
                            .orElse(null)).isEqualTo("Bearer my_refreshed_token");
                    }

                });
        }, errorResponse, normalResponse);

        // AccessToken only called 2 times on 3 attempts to send the messages
        verify(accessTokenHttpSender, times(2)).call();
    }

    @Test
    void throwsConnectExceptionForUnauthorizedToken() {

        // first call to retrieve an access token
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(ACCESS_TOKEN_RESPONSE);
        when(accessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse);

        // Mock response with 401 for all responses after the 1st one from super method
        final HttpResponse<String> errorResponse = mock(HttpResponse.class);
        when(errorResponse.statusCode()).thenReturn(401);

        assertThatExceptionOfType(ConnectException.class)
            .isThrownBy(() -> super.assertHttpSender(defaultConfig(), List.of("some message 1", "some message 2"),
                httpRequests -> {
                }, errorResponse))
            .withMessage("Sending failed and no retries remain, stopping");

        // Only 2 calls were made with 1 retry
        verify(accessTokenHttpSender, times(2)).call();
    }

    @Override
    void throwsConnectExceptionForServerError() {
        // first call to retrieve an access token
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(ACCESS_TOKEN_RESPONSE);
        when(accessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse);

        super.throwsConnectExceptionForServerError();
    }

    @Test
    void throwsConnectExceptionForWrongAuthentication() {

        // Bad formed json
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn("not a json");
        when(accessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse);

        assertThatExceptionOfType(ConnectException.class)
            .isThrownBy(() -> buildHttpSender(new HttpSinkConfig(defaultConfig()), null).send("a message"))
            .withMessage("Couldn't get OAuth2 access token");

        // Only 2 calls were made with 1 retry
        verify(accessTokenHttpSender, times(1)).call();
    }

    @Test
    void throwsConnectExceptionForBadFormedAccessToken() {

        // Unable to authenticate
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(
            "{\"bad_property\": \"my_access_token\",\"token_type\": \"Bearer\",\"expires_in\": 7199}");
        when(accessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse);

        assertThatExceptionOfType(ConnectException.class)
            .isThrownBy(() -> buildHttpSender(new HttpSinkConfig(defaultConfig()), null).send("a message"))
            .withMessage("Couldn't find access token property access_token in"
                         + " response properties: [bad_property, token_type, " + "expires_in]");

        verify(accessTokenHttpSender, times(1)).call();
    }

    protected Map<String, String> defaultConfig() {
        return Map.of("http.url", "http://localhost:42", "http.authorization.type", "oauth2", "oauth2.access.token.url",
            "http://localhost:42/token", "oauth2.client.id", "some_client_id", "oauth2.client.secret",
            "some_client_secret");
    }

}
