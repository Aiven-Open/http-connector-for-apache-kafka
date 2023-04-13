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

package io.aiven.kafka.connect.http.sender.request;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.StringJoiner;

import lombok.Builder;

@Builder
public class OAuth2Form {

    private static final String SCOPE = "scope";
    private final StringJoiner stringJoiner = new StringJoiner("&");

    private String grantTypeFormField;
    private String grantType;

    private String scope;
    private String clientIdFormField;
    private String clientId;

    private String clientSecretFormField;
    private String clientSecret;

    public String toString() {
        stringJoiner.add(encodeNameAndValue(grantTypeFormField, grantType));
        if (scope != null) {
            stringJoiner.add(encodeNameAndValue(SCOPE, scope));
        }
        if (clientId != null && clientSecret != null) {
            stringJoiner.add(encodeNameAndValue(clientIdFormField, clientId))
                        .add(encodeNameAndValue(clientSecretFormField, clientSecret));
        }
        return stringJoiner.toString();
    }

    private String encodeNameAndValue(final String name, final String value) {
        return String.format("%s=%s", encode(name), encode(value));
    }

    private static String encode(final String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

}
