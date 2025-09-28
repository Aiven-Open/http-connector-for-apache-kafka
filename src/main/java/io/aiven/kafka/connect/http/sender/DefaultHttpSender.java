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

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.Builder;
import java.time.Duration;

import io.aiven.kafka.connect.http.config.HttpMethodsType;
import io.aiven.kafka.connect.http.config.HttpSinkConfig;

class DefaultHttpSender extends AbstractHttpSender implements HttpSender {

    DefaultHttpSender(final HttpSinkConfig config, final HttpClient client) {
        super(config, new DefaultHttpRequestBuilder(), client, null);
    }

    static class DefaultHttpRequestBuilder implements HttpRequestBuilder {

        @Override
        public Builder build(final HttpSinkConfig config) {
            final var httpRequest = HttpRequest
                .newBuilder(config.httpUri())
                .timeout(Duration.ofSeconds(config.httpTimeout()));
            config
                .getAdditionalHeaders()
                .forEach(httpRequest::header);
            if (config.headerContentType() != null) {
                httpRequest.header(HEADER_CONTENT_TYPE, config.headerContentType());
            }
            return httpRequest;
        }

    }

}
