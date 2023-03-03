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

import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.Objects;
import java.util.function.BiConsumer;

import io.aiven.kafka.connect.http.config.AuthorizationType;
import io.aiven.kafka.connect.http.config.HttpSinkConfig;

interface HttpRequestBuilder {

    String HEADER_AUTHORIZATION = "Authorization";

    String HEADER_CONTENT_TYPE = "Content-Type";

    HttpRequest.Builder build(final HttpSinkConfig config);

    HttpRequestBuilder DEFAULT_HTTP_REQUEST_BUILDER = config -> {
        final var httpRequest = HttpRequest.newBuilder(config.httpUri())
                .timeout(Duration.ofSeconds(config.httpTimeout()));
        config.getAdditionalHeaders().forEach(httpRequest::header);
        if (config.headerContentType() != null) {
            httpRequest.header(HEADER_CONTENT_TYPE, config.headerContentType());
        }
        return httpRequest;
    };

    HttpRequestBuilder AUTH_HTTP_REQUEST_BUILDER = config -> DEFAULT_HTTP_REQUEST_BUILDER.build(config)
            .header(HEADER_AUTHORIZATION, config.headerAuthorization());

    interface OAuth2HttpRequestBuilder extends HttpRequestBuilder {
        String HEADER_CONTENT_TYPE_FORM = "application/x-www-form-urlencoded";
        BiConsumer<HttpSinkConfig, AuthorizationType> VALIDATE = (httpSinkConfig, authorizationType) -> {
            Objects.requireNonNull(httpSinkConfig, "config should not be null");
            if (httpSinkConfig.authorizationType() != authorizationType) {
                throw new IllegalArgumentException(String.format("The expected authorization type is %s",
                        authorizationType.name));
            }
        };
    }
}
