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

import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

public class HttpSenderFactory {

    public static HttpSender createHttpSender(final HttpSinkConfig config) {
        switch (config.authorizationType()) {
            case NONE:
                return new DefaultHttpSender(config, HttpClient.newHttpClient());
            case STATIC:
                return new StaticHttpSender(config, HttpClient.newHttpClient());
            case OAUTH2:
                final AccessTokenHttpSender accessTokenHttpSender =
                        new AccessTokenHttpSender(config, HttpClient.newHttpClient());
                return new OAuth2HttpSender(config, HttpClient.newHttpClient(), accessTokenHttpSender);
            default:
                throw new ConnectException("Can't create HTTP sender for auth type: " + config.authorizationType());
        }
    }

}
