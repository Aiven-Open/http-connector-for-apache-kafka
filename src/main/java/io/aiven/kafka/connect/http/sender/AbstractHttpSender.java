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

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import io.aiven.kafka.connect.http.config.HttpMethodsType;
import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractHttpSender {

    private static final Logger log = LoggerFactory.getLogger(AbstractHttpSender.class);

    protected final HttpClient httpClient;
    protected final HttpSinkConfig config;
    protected final HttpMethodsType method;
    protected final HttpRequestBuilder httpRequestBuilder;

    protected AbstractHttpSender(
            final HttpSinkConfig config,
            final HttpRequestBuilder httpRequestBuilder,
            final HttpClient httpClient,
            final HttpMethodsType method
    ) {
        this.config = Objects.requireNonNull(config);
        this.httpRequestBuilder = Objects.requireNonNull(httpRequestBuilder);
        this.httpClient = Objects.requireNonNull(httpClient);
        this.method = method;
    }

    public final HttpResponse<String> send(final String body) {
        final var requestBuilder = prepareRequest(body);
        return sendWithRetries(requestBuilder, HttpResponseHandler.ON_HTTP_ERROR_RESPONSE_HANDLER,
                config.maxRetries());
    }

    // seth http bethod based on config
    private Builder prepareRequest(final String body) {
        if(method == null) {
            switch (config.httpMethod()) {
                case POST:
                    return httpRequestBuilder
                            .build(config).POST(HttpRequest.BodyPublishers.ofString(body));
                case PUT:
                    return httpRequestBuilder
                            .build(config).PUT(HttpRequest.BodyPublishers.ofString(body));
                default:
                    throw new ConnectException("Unsupported HTTP method: " + config.httpMethod());
            }
        } else
            return httpRequestBuilder
                    .build(config).POST(HttpRequest.BodyPublishers.ofString(body));

    }

    /**
     * Sends an HTTP body using {@code httpSender}, respecting the configured retry policy.
     *
     * @return whether the sending was successful.
     */
    protected HttpResponse<String> sendWithRetries(
            final Builder requestBuilderWithPayload, final HttpResponseHandler httpResponseHandler,
            final int retries
    ) {
        int remainingRetries = retries;
        while (remainingRetries >= 0) {
            try {
                try {
                    final var response =
                            httpClient.send(requestBuilderWithPayload.build(), HttpResponse.BodyHandlers.ofString());
                    log.debug("Server replied with status code {} and body {}", response.statusCode(), response.body());
                    // Handle the response
                    httpResponseHandler.onResponse(response, remainingRetries);
                    return response;
                } catch (final IOException e) {
                    log.info("Sending failed, will retry in {} ms ({} retries remain)", config.retryBackoffMs(),
                            remainingRetries, e);
                    remainingRetries -= 1;
                    TimeUnit.MILLISECONDS.sleep(config.retryBackoffMs());
                }
            } catch (final InterruptedException e) {
                log.error("Sending failed due to InterruptedException, stopping", e);
                throw new ConnectException(e);
            }
        }
        log.error("Sending failed and no retries remain, stopping");
        throw new ConnectException("Sending failed and no retries remain, stopping");
    }

}
