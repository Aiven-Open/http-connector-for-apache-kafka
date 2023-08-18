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

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpRequest;
import java.time.Duration;

import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

interface HttpRequestBuilder {

    String HEADER_AUTHORIZATION = "Authorization";

    String HEADER_CONTENT_TYPE = "Content-Type";

    HttpRequest.Builder build(final HttpSinkConfig config, final String key);

    HttpRequestBuilder DEFAULT_HTTP_REQUEST_BUILDER = (config, key) -> {
        URI reqUri = config.httpUri();
        try {
            if (config.updateUrlEnabled() && config.httpUpdateUrl() != null && key != null) {
                reqUri = new URL(config.httpUpdateUrl().replace("${key}", key)).toURI();
            }
        } catch (final MalformedURLException | URISyntaxException e) {
            throw new ConnectException(e);
        }

        final var httpRequest = HttpRequest.newBuilder(reqUri)
                .timeout(Duration.ofSeconds(config.httpTimeout()));
        config.getAdditionalHeaders().forEach(httpRequest::header);
        if (config.headerContentType() != null) {
            httpRequest.header(HEADER_CONTENT_TYPE, config.headerContentType());
        }
        return httpRequest;
    };

    HttpRequestBuilder AUTH_HTTP_REQUEST_BUILDER = (config, key) -> DEFAULT_HTTP_REQUEST_BUILDER.build(config, key)
            .header(HEADER_AUTHORIZATION, config.headerAuthorization());

}
