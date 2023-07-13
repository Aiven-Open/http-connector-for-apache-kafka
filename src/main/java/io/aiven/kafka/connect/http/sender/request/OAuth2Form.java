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

public class OAuth2Form {

    private static final String SCOPE = "scope";
    private final StringJoiner stringJoiner = new StringJoiner("&");

    private final String grantTypeFormField;
    private final String grantType;

    private final String scope;
    private final String clientIdFormField;
    private final String clientId;

    private final String clientSecretFormField;
    private final String clientSecret;

    private OAuth2Form(
        final String grantTypeFormField,
        final String grantType,
        final String scope,
        final String clientIdFormField,
        final String clientId,
        final String clientSecretFormField,
        final String clientSecret
    ) {
        this.grantTypeFormField = grantTypeFormField;
        this.grantType = grantType;
        this.scope = scope;
        this.clientIdFormField = clientIdFormField;
        this.clientId = clientId;
        this.clientSecretFormField = clientSecretFormField;
        this.clientSecret = clientSecret;
    }

    public String toBodyString() {
        stringJoiner.add(encodeNameAndValue(grantTypeFormField, grantType));
        if (scope != null) {
            stringJoiner.add(encodeNameAndValue(SCOPE, scope));
        }
        if (clientId != null && clientSecret != null) {
            stringJoiner
                .add(encodeNameAndValue(clientIdFormField, clientId))
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

    public static class Builder {

        private String grantTypeFormField;
        private String grantType;

        private String scope;
        private String clientIdFormField;
        private String clientId;

        private String clientSecretFormField;
        private String clientSecret;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder grantTypeFormField(final String grantTypeFormField) {
            this.grantTypeFormField = grantTypeFormField;
            return this;
        }

        public Builder grantType(final String grantType) {
            this.grantType = grantType;
            return this;
        }

        public Builder scope(final String scope) {
            this.scope = scope;
            return this;
        }

        public Builder clientIdFormField(final String clientIdFormField) {
            this.clientIdFormField = clientIdFormField;
            return this;
        }

        public Builder clientId(final String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder clientSecretFormField(final String clientSecretFormField) {
            this.clientSecretFormField = clientSecretFormField;
            return this;
        }

        public Builder clientSecret(final String clientSecret) {
            this.clientSecret = clientSecret;
            return this;
        }

        public OAuth2Form build() {
            return new OAuth2Form(grantTypeFormField, grantType, scope, clientIdFormField, clientId,
                clientSecretFormField, clientSecret);
        }

    }

}
