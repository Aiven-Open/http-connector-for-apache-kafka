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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public abstract class HttpSenderTestUtils<T extends AbstractHttpSender> {

    @Mock
    protected HttpClient mockedClient;

    @Mock
    private HttpResponse<String> mockedResponse;

    private static MockedStatic<BodyPublishers> bodyPublishers;

    protected abstract T buildHttpSender(HttpSinkConfig config, HttpClient client);

    @BeforeAll
    public static void setup() {
        final BodyPublisher bodyPublisher = Mockito.mock(BodyPublisher.class);
        bodyPublishers = Mockito.mockStatic(HttpRequest.BodyPublishers.class);
        bodyPublishers
            .when(() -> HttpRequest.BodyPublishers.ofString(anyString()))
            .thenReturn(bodyPublisher);
    }

    @AfterEach
    public void clear() {
        bodyPublishers.clearInvocations();
    }

    @AfterAll
    public static void clean() {
        bodyPublishers.close();
    }

    @Test
    void shouldThrowExceptionWithoutConfig() {
        final Exception thrown = assertThrows(NullPointerException.class, () -> new DefaultHttpSender(null, null));
        assertEquals("config should not be null", thrown.getMessage());
    }

    @SafeVarargs
    protected final void assertHttpSender(
        final Map<String, String> configBase,
        final List<String> messages,
        final Consumer<List<HttpRequest>> requestAssertor,
        final HttpResponse<String>... otherMockedResponses
    ) throws IOException, InterruptedException {

        // Build the configuration
        final HttpSinkConfig config = new HttpSinkConfig(configBase);

        // Mock the Client and Response
        when(mockedClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(mockedResponse,
            otherMockedResponses);

        // Create a spy on the HttpSender implementation to capture methods parameters
        final var httpSender = Mockito.spy(buildHttpSender(config, mockedClient));

        // Trigger the client
        messages.forEach(message -> this.sendMessage(httpSender, message));

        // Capture the RequestBuilder
        final ArgumentCaptor<HttpRequest.Builder> defaultHttpRequestBuilder =
            ArgumentCaptor.forClass(HttpRequest.Builder.class);
        verify(httpSender, atLeast(messages.size())).sendWithRetries(defaultHttpRequestBuilder.capture(),
            any(HttpResponseHandler.class), anyInt());
        defaultHttpRequestBuilder
            .getAllValues()
            .stream()
            .map(Builder::build)
            .forEach(builder -> {
                // Generic Assertions
                assertThat(builder.uri()).isEqualTo(this.getTargetURI(config));
                assertThat(builder.timeout())
                    .isPresent()
                    .get(as(InstanceOfAssertFactories.DURATION))
                    .hasSeconds(config.httpTimeout());
                assertThat(builder.method()).isEqualTo("POST");
            });

        // Run specific assertions
        requestAssertor.accept(defaultHttpRequestBuilder
            .getAllValues()
            .stream()
            .map(Builder::build)
            .collect(Collectors.toList()));

        // Test the Body message from the argument captor
        messages.forEach(
            message -> bodyPublishers.verify(() -> HttpRequest.BodyPublishers.ofString(eq(message)), times(1)));
    }

    @Test
    void throwsConnectExceptionForServerError() {
        // Mock response with 500 for all responses after the 1st one from super method
        final HttpResponse<String> errorResponse = mock(HttpResponse.class);
        when(errorResponse.statusCode()).thenReturn(500);

        assertThatExceptionOfType(ConnectException.class)
            .isThrownBy(() -> this.assertHttpSender(defaultConfig(), List.of("some message 1", "some message 2"),
                httpRequests -> {
                }, errorResponse))
            .withMessage("Sending failed and no retries remain, stopping");
    }

    protected void sendMessage(final HttpSender httpSender, final String message) {
        httpSender.send(message);
    }

    protected URI getTargetURI(final HttpSinkConfig config) {
        return config.httpUri();
    }

    protected abstract Map<String, String> defaultConfig();

}
