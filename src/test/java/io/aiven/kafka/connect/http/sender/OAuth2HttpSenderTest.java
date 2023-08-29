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
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OAuth2HttpSenderTest extends HttpSenderTestBase<OAuth2HttpSender> {

    static final String ACCESS_TOKEN_RESPONSE =
        "{\"access_token\": \"my_access_token\",\"token_type\": \"Bearer\",\"expires_in\": 7199}";
    @Mock
    private OAuth2AccessTokenHttpSender oauth2AccessTokenHttpSender;

    @Test
    void shouldThrowExceptionWithoutConfig() {
        assertThrows(NullPointerException.class, () -> new OAuth2HttpSender(null, null, null));
    }

    @Test
    void shouldBuildDefaultHttpRequest() throws Exception {
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(ACCESS_TOKEN_RESPONSE);
        when(oauth2AccessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse);

        // Build the configuration
        final HttpSinkConfig config = new HttpSinkConfig(defaultConfig());

        // Mock the Client and Response
        when(mockedClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(mockedResponse);

        // Create a spy on the HttpSender implementation to capture methods parameters
        final var httpSender = Mockito.spy(new OAuth2HttpSender(config, mockedClient, oauth2AccessTokenHttpSender));

        // Trigger the client
        final List<String> messages = List.of("some message");
        messages.forEach(httpSender::send);

        // Capture the RequestBuilder
        final ArgumentCaptor<Builder> defaultHttpRequestBuilder = ArgumentCaptor.forClass(HttpRequest.Builder.class);
        verify(httpSender, atLeast(messages.size())).sendWithRetries(defaultHttpRequestBuilder.capture(),
            any(HttpResponseHandler.class), anyInt());

        // Retrieve the builders and rebuild the HttpRequests to check the HttpRequest proper configuration
        defaultHttpRequestBuilder
            .getAllValues()
            .stream()
            .map(Builder::build)
            .forEach(httpRequest -> {
                // Generic Assertions
                assertThat(httpRequest.uri()).isEqualTo(config.httpUri());
                assertThat(httpRequest.timeout())
                    .isPresent()
                    .get(as(InstanceOfAssertFactories.DURATION))
                    .hasSeconds(config.httpTimeout());
                assertThat(httpRequest.method()).isEqualTo("POST");

                assertThat(httpRequest
                    .headers()
                    .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)
                    .orElse(null)).isEqualTo("Bearer my_access_token");
            });

        // Check the messages have been sent once
        messages.forEach(
            message -> bodyPublishers.verify(() -> HttpRequest.BodyPublishers.ofString(eq(message)), times(1)));
        // Httpclient is called once per message
        verify(mockedClient, times(messages.size())).send(any(HttpRequest.class), any(BodyHandler.class));
    }

    @Test
    void buildAccessTokenAuthHeaderFromCustomSettings() throws IOException, InterruptedException {

        final String basicTokenResponse = "{\"some_token\": \"my_basic_token\",\"token_type\": \"Basic\"}";

        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(basicTokenResponse);
        when(oauth2AccessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse);

        final var configWithToken = new HashMap<>(defaultConfig());
        configWithToken.put("oauth2.response.token.property", "some_token");

        // Build the configuration
        final HttpSinkConfig config = new HttpSinkConfig(configWithToken);

        // Mock the Client and Response
        when(mockedClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(mockedResponse);

        // Create a spy on the HttpSender implementation to capture methods parameters
        final var httpSender = Mockito.spy(new OAuth2HttpSender(config, mockedClient, oauth2AccessTokenHttpSender));

        // Trigger the client
        final List<String> messages = List.of("some message");
        messages.forEach(httpSender::send);

        // Capture the RequestBuilder
        final ArgumentCaptor<Builder> defaultHttpRequestBuilder = ArgumentCaptor.forClass(HttpRequest.Builder.class);
        verify(httpSender, atLeast(messages.size())).sendWithRetries(defaultHttpRequestBuilder.capture(),
            any(HttpResponseHandler.class), anyInt());

        // Retrieve the builders and rebuild the HttpRequests to check the HttpRequest proper configuration
        defaultHttpRequestBuilder
            .getAllValues()
            .stream()
            .map(Builder::build)
            .forEach(httpRequest -> {
                assertThat(httpRequest.uri()).isEqualTo(config.httpUri());
                assertThat(httpRequest.timeout())
                    .isPresent()
                    .get(as(InstanceOfAssertFactories.DURATION))
                    .hasSeconds(config.httpTimeout());
                assertThat(httpRequest.method()).isEqualTo("POST");

                assertThat(httpRequest
                    .headers()
                    .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)
                    .orElse(null)).isEqualTo("Basic my_basic_token");
            });

        // Check the messages have been sent once
        messages.forEach(
            message -> bodyPublishers.verify(() -> HttpRequest.BodyPublishers.ofString(eq(message)), times(1)));
        // Httpclient is called once per message
        verify(mockedClient, times(messages.size())).send(any(HttpRequest.class), any(BodyHandler.class));

    }

    @Test
    void reuseAccessToken() throws Exception {
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(ACCESS_TOKEN_RESPONSE);
        when(oauth2AccessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse);

        // Build the configuration
        final HttpSinkConfig config = new HttpSinkConfig(defaultConfig());

        // Mock the Client and Response
        when(mockedClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(mockedResponse);

        // Create a spy on the HttpSender implementation to capture methods parameters
        final var httpSender = Mockito.spy(new OAuth2HttpSender(config, mockedClient, oauth2AccessTokenHttpSender));

        // Trigger the client
        final List<String> messages = List.of("some message 1", "some message 2");
        messages.forEach(httpSender::send);

        // Capture the RequestBuilder
        final ArgumentCaptor<Builder> defaultHttpRequestBuilder = ArgumentCaptor.forClass(HttpRequest.Builder.class);
        verify(httpSender, atLeast(messages.size())).sendWithRetries(defaultHttpRequestBuilder.capture(),
            any(HttpResponseHandler.class), anyInt());

        // Retrieve the builders and rebuild the HttpRequests to check the HttpRequest proper configuration
        defaultHttpRequestBuilder
            .getAllValues()
            .stream()
            .map(Builder::build)
            .forEach(httpRequest -> {
                // Generic Assertions
                assertThat(httpRequest.uri()).isEqualTo(config.httpUri());
                assertThat(httpRequest.timeout())
                    .isPresent()
                    .get(as(InstanceOfAssertFactories.DURATION))
                    .hasSeconds(config.httpTimeout());
                assertThat(httpRequest.method()).isEqualTo("POST");

                assertThat(httpRequest
                    .headers()
                    .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)
                    .orElse(null)).isEqualTo("Bearer my_access_token");
            });

        verify(oauth2AccessTokenHttpSender, times(1)).call();

        // Check the messages have been sent once
        messages.forEach(
            message -> bodyPublishers.verify(() -> HttpRequest.BodyPublishers.ofString(eq(message)), times(1)));
        // Httpclient is called once per message
        verify(mockedClient, times(messages.size())).send(any(HttpRequest.class), any(BodyHandler.class));

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
        when(oauth2AccessTokenHttpSender.call()).thenReturn(
            mockedAccessTokenResponse, mockedAccessTokenResponseRefreshed);

        // Mock a 2nd response with 401.
        final HttpResponse<String> errorResponse = mock(HttpResponse.class);
        when(errorResponse.statusCode()).thenReturn(401);
        // Mock a 2nd response with 401.
        final HttpResponse<String> normalResponse = mock(HttpResponse.class);
        when(normalResponse.statusCode()).thenReturn(200);

        // Build the configuration
        final HttpSinkConfig config = new HttpSinkConfig(defaultConfig());

        // Mock the Client and Response
        when(mockedClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(mockedResponse,
            errorResponse, normalResponse);

        // Create a spy on the HttpSender implementation to capture methods parameters
        final var httpSender = Mockito.spy(new OAuth2HttpSender(config, mockedClient, oauth2AccessTokenHttpSender));

        // Trigger the client
        final List<String> messages = List.of("some message 1", "some message 2");
        messages.forEach(httpSender::send);

        // Capture the RequestBuilder
        final ArgumentCaptor<Builder> defaultHttpRequestBuilder = ArgumentCaptor.forClass(HttpRequest.Builder.class);
        verify(httpSender, atLeast(messages.size())).sendWithRetries(defaultHttpRequestBuilder.capture(),
            any(HttpResponseHandler.class), anyInt());

        // Retrieve the builders and rebuild the HttpRequests to check the HttpRequest proper configuration
        final List<HttpRequest> httpRequests = defaultHttpRequestBuilder
            .getAllValues()
            .stream()
            .map(Builder::build)
            .collect(Collectors.toList());

        // 3 attempts were made
        assertThat(httpRequests.size()).isEqualTo(3);
        IntStream
            .of(httpRequests.size() - 1)
            .boxed()
            .forEach(httpRequestIndex -> {
                final HttpRequest httpRequest = httpRequests.get(httpRequestIndex);

                assertThat(httpRequest.uri()).isEqualTo(config.httpUri());
                assertThat(httpRequest.timeout())
                    .isPresent()
                    .get(as(InstanceOfAssertFactories.DURATION))
                    .hasSeconds(config.httpTimeout());
                assertThat(httpRequest.method()).isEqualTo("POST");

                // First time the access token is my_access_token
                if (httpRequestIndex == 0) {
                    assertThat(httpRequest
                        .headers()
                        .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)
                        .orElse(null)).isEqualTo("Bearer my_access_token");
                } else {
                    // Every other calls are with my_refreshed_token
                    assertThat(httpRequest
                        .headers()
                        .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)
                        .orElse(null)).isEqualTo("Bearer my_refreshed_token");
                }

            });

        // AccessToken only called 2 times on 3 attempts to send the messages
        verify(oauth2AccessTokenHttpSender, times(2)).call();

        // Check the messages have been sent once
        messages.forEach(
            message -> bodyPublishers.verify(() -> HttpRequest.BodyPublishers.ofString(eq(message)), times(1)));
        // Httpclient is called once per message and once more after having refreshed the token
        verify(mockedClient, times(messages.size() + 1)).send(any(HttpRequest.class), any(BodyHandler.class));
    }

    @Test
    void throwsConnectExceptionForUnauthorizedToken() {

        // first call to retrieve an access token
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(ACCESS_TOKEN_RESPONSE);
        when(oauth2AccessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse);

        // Mock response with 401 for all responses after the 1st one from super method
        final HttpResponse<String> errorResponse = mock(HttpResponse.class);
        when(errorResponse.statusCode()).thenReturn(401);

        assertThatExceptionOfType(ConnectException.class)
            .isThrownBy(() -> {
                // Build the configuration
                final HttpSinkConfig config = new HttpSinkConfig(defaultConfig());

                // Mock the Client and Response
                when(mockedClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(errorResponse);

                // Create a spy on the HttpSender implementation to capture methods parameters
                final var httpSender =
                    Mockito.spy(new OAuth2HttpSender(config, mockedClient, oauth2AccessTokenHttpSender));

                // Trigger the client
                final List<String> messages = List.of("some message 1", "some message 2");
                messages.forEach(httpSender::send);

            })
            .withMessage("Sending failed and no retries remain, stopping");

        // Only 2 calls were made with 1 retry
        verify(oauth2AccessTokenHttpSender, times(2)).call();
    }

    @Test
    void throwsConnectExceptionForWrongAuthentication() throws IOException, InterruptedException {

        // Bad formed json
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn("not a json");
        when(oauth2AccessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse);

        assertThatExceptionOfType(ConnectException.class)
            .isThrownBy(() -> new OAuth2HttpSender(new HttpSinkConfig(defaultConfig()), mockedClient,
                oauth2AccessTokenHttpSender).send("a message"))
            .withMessage("Couldn't get OAuth2 access token");

        // Only 2 calls were made with 1 retry
        verify(oauth2AccessTokenHttpSender, times(1)).call();
        // Httpclient is never called to send the message
        verify(mockedClient, never()).send(any(HttpRequest.class), any(BodyHandler.class));
    }

    @Test
    void throwsConnectExceptionForBadFormedAccessToken() throws IOException, InterruptedException {

        // Unable to authenticate
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(
            "{\"bad_property\": \"my_access_token\",\"token_type\": \"Bearer\",\"expires_in\": 7199}");
        when(oauth2AccessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse);

        assertThatExceptionOfType(ConnectException.class)
            .isThrownBy(() -> new OAuth2HttpSender(new HttpSinkConfig(defaultConfig()), mockedClient,
                oauth2AccessTokenHttpSender).send("a message"))
            .withMessage("Couldn't find access token property access_token in"
                         + " response properties: [bad_property, token_type, expires_in]");

        verify(oauth2AccessTokenHttpSender, times(1)).call();
        // Httpclient is never called to send the message
        verify(mockedClient, never()).send(any(HttpRequest.class), any(BodyHandler.class));
    }

    @Test
    void throwsConnectExceptionForServerError() {
        // first call to retrieve an access token
        final HttpResponse<String> mockedAccessTokenResponse = mock(HttpResponse.class);
        when(mockedAccessTokenResponse.body()).thenReturn(ACCESS_TOKEN_RESPONSE);
        when(oauth2AccessTokenHttpSender.call()).thenReturn(mockedAccessTokenResponse);

        // Mock response with 500 for all responses after the 1st one from super method
        final HttpResponse<String> errorResponse = mock(HttpResponse.class);
        when(errorResponse.statusCode()).thenReturn(500);

        assertThatExceptionOfType(ConnectException.class)
            .isThrownBy(() -> {
                // Build the configuration
                final HttpSinkConfig config = new HttpSinkConfig(defaultConfig());

                // Mock the Client and Response
                when(mockedClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(errorResponse);

                // Create a spy on the HttpSender implementation to capture methods parameters
                final var httpSender =
                    Mockito.spy(new OAuth2HttpSender(config, mockedClient, oauth2AccessTokenHttpSender));

                // Trigger the client
                final List<String> messages = List.of("some message 1", "some message 2");
                messages.forEach(httpSender::send);

            })
            .withMessage("Sending failed and no retries remain, stopping");
    }

    private Map<String, String> defaultConfig() {
        return Map.of("http.url", "http://localhost:42", "http.authorization.type", "oauth2", "oauth2.access.token.url",
            "http://localhost:42/token", "oauth2.client.id", "some_client_id", "oauth2.client.secret",
            "some_client_secret");
    }

}
