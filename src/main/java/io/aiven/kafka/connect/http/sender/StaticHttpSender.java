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

import java.net.http.HttpClient;
import java.net.http.HttpRequest.Builder;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;
import io.aiven.kafka.connect.http.sender.DefaultHttpSender.DefaultHttpRequestBuilder;

class StaticHttpSender extends AbstractHttpSender implements HttpSender {

    StaticHttpSender(final HttpSinkConfig config, final HttpClient client) {
        super(config, new StaticAuthHttpRequestBuilder(), client);
    }

    private static class StaticAuthHttpRequestBuilder extends DefaultHttpRequestBuilder {

        @Override
        public Builder build(final HttpSinkConfig config) {
            return super
                .build(config)
                .header(HEADER_AUTHORIZATION, config.headerAuthorization());
        }

    }

}
