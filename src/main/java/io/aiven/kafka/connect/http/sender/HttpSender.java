/*
 * Copyright 2019 Aiven Oy
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
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class HttpSender {

    private static final Logger log = LoggerFactory.getLogger(HttpSender.class);

    protected static final String HEADER_AUTHORIZATION = "Authorization";

    protected static final String HEADER_CONTENT_TYPE = "Content-Type";

    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(30);

    private final HttpClient httpClient;

    protected final HttpRequest.Builder requestTemplate;

    protected final HttpSinkConfig config;

    protected interface OnResponseHandler {

        void onResponse(final HttpResponse<String> response) throws IOException;

    }

    protected final OnResponseHandler onHttpErrorResponseHandler = response -> {
        if (response.statusCode() >= 400) {
            if (response.statusCode() < 200 || response.statusCode() > 299) {
                log.warn("Got unexpected HTTP status code: {}", response.statusCode());
            }
            throw new IOException("Server replied with status code " + response.statusCode()
                    + " and body " + response.body());
        }
    };

    protected HttpSender(final HttpSinkConfig config) {
        this.config = config;
        this.httpClient = HttpClient.newHttpClient();
        try {
            requestTemplate = HttpRequest.newBuilder(config.httpUrl().toURI()).timeout(HTTP_TIMEOUT);
        } catch (final URISyntaxException e) {
            throw new ConnectException(e);
        }
    }

    public final void send(final String body) {
        final HttpRequest request = requestTemplate.copy()
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        sendWithRetries(request, onHttpErrorResponseHandler);
    }

    /**
     * Sends a HTTP body using {@code httpSender}, respecting the configured retry policy.
     *
     * @return whether the sending was successful.
     */
    protected void sendWithRetries(final HttpRequest request,
                                   final OnResponseHandler onHttpResponseHandler) {
        int remainRetries = config.maxRetries();
        while (remainRetries >= 0) {
            try {
                try {
                    final var response =
                            httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                    log.debug("Server replied with status code {} and body {}", response.statusCode(), response.body());
                    onHttpResponseHandler.onResponse(response);
                    return;
                } catch (final IOException e) {
                    log.info("Sending failed, will retry in {} ms ({} retries remain)",
                            config.retryBackoffMs(), remainRetries, e);
                    remainRetries -= 1;
                    TimeUnit.MILLISECONDS.sleep(config.retryBackoffMs());
                }
            } catch (final InterruptedException e) {
                log.error("Sending failed due to InterruptedException, stopping");
                throw new ConnectException(e);
            }
        }
        log.error("Sending failed and no retries remain, stopping");
        throw new ConnectException("Sending failed and no retries remain, stopping");
    }

    public static HttpSender createHttpSender(final HttpSinkConfig config) {
        switch (config.authorizationType()) {
            case STATIC:
                return new StaticAuthHttpSender(config);
            case NONE:
                return new NoAuthHttpSender(config);
            default:
                throw new ConnectException("Can't create HTTP sender for auth type: " + config.authorizationType());
        }
    }

    private static final class NoAuthHttpSender extends HttpSender {

        private NoAuthHttpSender(final HttpSinkConfig config) {
            super(config);
        }

    }

    private static final class StaticAuthHttpSender extends HttpSender {

        private StaticAuthHttpSender(final HttpSinkConfig config) {
            super(config);
            requestTemplate.header(HEADER_AUTHORIZATION, config.headerAuthorization());
            if (config.headerContentType() != null) {
                requestTemplate.header(HEADER_CONTENT_TYPE, config.headerContentType());
            }
        }
    }

}
