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
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;
import io.aiven.kafka.connect.http.config.OAuth2AuthorizationMode;
import io.aiven.kafka.connect.http.sender.request.OAuth2Form;
import io.aiven.kafka.connect.http.sender.request.OAuth2Form.OAuth2FormBuilder;

import static io.aiven.kafka.connect.http.config.OAuth2AuthorizationMode.HEADER;

class AccessTokenHttpSender extends AbstractHttpSender {

    AccessTokenHttpSender(final HttpSinkConfig config, final HttpClient httpClient) {
        super(config, new AccessTokenHttpRequestBuilder(), httpClient);
    }

    HttpResponse<String> call() {
        final OAuth2FormBuilder bodyBuilder = OAuth2Form.builder()
                                                  .grantTypeFormField(config.oauth2GrantTypeKey())
                                                  .grantType(config.oauth2GrantType())
                                                  .scope(config.oauth2ClientScope());

        if (config.oauth2AuthorizationMode() == OAuth2AuthorizationMode.URL) {
            bodyBuilder.clientIdFormField(config.oauth2ClientIdKey())
                       .clientId(config.oauth2ClientId())
                       .clientSecretFormField(config.oauth2ClientSecretKey())
                       .clientSecret(config.oauth2ClientSecret().value());
        }
        return super.send(bodyBuilder.build().toString());
    }

    private static class AccessTokenHttpRequestBuilder implements HttpRequestBuilder {

        static final String HEADER_CONTENT_TYPE_FORM = "application/x-www-form-urlencoded";

        protected AccessTokenHttpRequestBuilder() {
        }

        @Override
        public HttpRequest.Builder build(final HttpSinkConfig config) {
            final var builder = HttpRequest.newBuilder(config.oauth2AccessTokenUri())
                                     .timeout(Duration.ofSeconds(config.httpTimeout()))
                                     .header(HEADER_CONTENT_TYPE, HEADER_CONTENT_TYPE_FORM);
            if (config.oauth2AuthorizationMode() == HEADER) {
                addClientIdAndSecretInRequestHeader(config, builder);
            }
            return builder;
        }

        private void addClientIdAndSecretInRequestHeader(
                final HttpSinkConfig config, final HttpRequest.Builder builder
        ) {
            final var clientAndSecretBytes =
                    (config.oauth2ClientId() + ":" + config.oauth2ClientSecret().value()).getBytes(
                            StandardCharsets.UTF_8);
            final var clientAndSecretAuthHeader = "Basic " + Base64.getEncoder().encodeToString(clientAndSecretBytes);
            builder.header(HEADER_AUTHORIZATION, clientAndSecretAuthHeader);
        }

    }

}
