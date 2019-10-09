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

package io.aiven.kafka.connect.http;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class HttpSender {
    private static final Logger log = LoggerFactory.getLogger(HttpSender.class);

    private static final String HEADER_AUTHORIZATION = "Authorization";
    private static final String HEADER_CONTENT_TYPE = "Content-Type";

    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(30);

    private final HttpClient httpClient;
    private final HttpRequest.Builder requestTemplate;

    HttpSender(final URL url,
               final String headerAuthorization,
               final String headerContentType) {
        this.httpClient = HttpClient.newHttpClient();

        final URI uri;
        try {
            uri = url.toURI();
        } catch (final URISyntaxException e) {
            throw new ConnectException(e);
        }
        requestTemplate = HttpRequest.newBuilder(uri)
            .timeout(HTTP_TIMEOUT);
        if (headerAuthorization != null) {
            requestTemplate.header(HEADER_AUTHORIZATION, headerAuthorization);
        }
        if (headerContentType != null) {
            requestTemplate.header(HEADER_CONTENT_TYPE, headerContentType);
        }
    }

    void sendBatch(final String batch) {
        final HttpRequest request = requestTemplate.copy()
            .POST(HttpRequest.BodyPublishers.ofString(batch))
            .build();
        final HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            log.debug("Server replied with status code {} and body {}", response.statusCode(), response.body());
        } catch (final IOException e) {
            throw new RetriableException(e);
        } catch (final InterruptedException e) {
            throw new ConnectException(e);
        }

        throwIfClientError(response);
        throwIfServerError(response);
    }

    private void throwIfClientError(final HttpResponse<String> response) {
        if (response.statusCode() >= 400 && response.statusCode() < 500) {
            throw new ConnectException("Server replied with status code " + response.statusCode()
                + " and body " + response.body());
        }
    }

    private void throwIfServerError(final HttpResponse<String> response) {
        if (response.statusCode() >= 500) {
            throw new RetriableException("Server replied with status code " + response.statusCode()
                + " and body " + response.body());
        }
    }
}
