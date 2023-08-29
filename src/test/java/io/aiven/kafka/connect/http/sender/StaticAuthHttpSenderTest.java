/*
 * Copyright 2023 Aiven Oy and http-connector-for-apache-kafka project contributors
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
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StaticAuthHttpSenderTest extends HttpSenderTestBase<StaticAuthHttpSender> {

    @Test
    void shouldThrowExceptionWithoutConfig() {
        assertThrows(NullPointerException.class, () -> new StaticAuthHttpSender(null, null));
    }

    @Test
    void shouldBuildDefaultStaticHttpRequest() throws Exception {

        // Build the configuration
        final HttpSinkConfig config = new HttpSinkConfig(defaultConfig());

        // Mock the Client and Response
        when(mockedClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(mockedResponse);

        // Create a spy on the HttpSender implementation to capture methods parameters
        final var httpSender = Mockito.spy(new StaticAuthHttpSender(config, mockedClient));

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
                    .firstValue(HttpRequestBuilder.HEADER_CONTENT_TYPE)).isEmpty();
                assertThat(httpRequest
                    .headers()
                    .firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)
                    .orElse(null)).isEqualTo("Bearer myToken");
            });

        // Check the message have been sent once
        messages.forEach(
            message -> bodyPublishers.verify(() -> HttpRequest.BodyPublishers.ofString(eq(message)), times(1)));

    }

    @Test
    void shouldBuildCustomStaticHttpRequest() throws Exception {
        final var configBase = new HashMap<>(defaultConfig());
        configBase.put("http.headers.content.type", "application/json");
        configBase.put("http.headers.additional", "header1:value1,header2:value2");

        // Build the configuration
        final HttpSinkConfig config = new HttpSinkConfig(configBase);

        // Mock the Client and Response
        when(mockedClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(mockedResponse);

        // Create a spy on the HttpSender implementation to capture methods parameters
        final var httpSender = Mockito.spy(new StaticAuthHttpSender(config, mockedClient));

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
                    .orElse(null)).isEqualTo("Bearer myToken");
                assertThat(httpRequest
                    .headers()
                    .firstValue(HttpRequestBuilder.HEADER_CONTENT_TYPE)
                    .orElse(null)).isEqualTo("application/json");
                assertThat(httpRequest
                    .headers()
                    .firstValue("header1")
                    .orElse(null)).isEqualTo("value1");
                assertThat(httpRequest
                    .headers()
                    .firstValue("header2")
                    .orElse(null)).isEqualTo("value2");
            });

        // Check the messages have been sent once
        messages.forEach(
            message -> bodyPublishers.verify(() -> HttpRequest.BodyPublishers.ofString(eq(message)), times(1)));

    }

    @Test
    void throwsConnectExceptionForServerError() {
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
                final var httpSender = Mockito.spy(new StaticAuthHttpSender(config, mockedClient));

                // Trigger the client
                final List<String> messages = List.of("some message 1", "some message 2");
                messages.forEach(httpSender::send);

            })
            .withMessage("Sending failed and no retries remain, stopping");
    }

    private Map<String, String> defaultConfig() {
        return Map.of("http.url", "http://localhost:42", "http.authorization.type", "static",
            "http.headers.authorization", "Bearer myToken");
    }

}


