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

package io.aiven.kafka.connect.http.sender.request;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class OAuth2AccessTokenRequestFormTest {

    @Test
    void buildShouldThrowAnErrorWithoutGrantTypeProperty() {
        assertThatExceptionOfType(NullPointerException.class)
            .isThrownBy(() -> OAuth2AccessTokenRequestForm
                .newBuilder()
                .build())
            .withMessage("The grant type property is required");
    }

    @Test
    void buildShouldThrowAnErrorWithoutGrantType() {
        assertThatExceptionOfType(NullPointerException.class)
            .isThrownBy(() -> OAuth2AccessTokenRequestForm
                .newBuilder()
                .withGrantTypeProperty("grant_type")
                .build())
            .withMessage("The grant type is required");
    }

    @Test
    void toBodyStringShouldReturnAValidFormWithOnlyGrantType() {
        assertThat(OAuth2AccessTokenRequestForm
            .newBuilder()
            .withGrantTypeProperty("grant_type")
            .withGrantType("client_credentials")
            .build()
            .toBodyString()).isEqualTo("grant_type=client_credentials");
    }

    @Test
    void toBodyStringShouldReturnAValidFormWithGrantTypeAndScope() {
        assertThat(OAuth2AccessTokenRequestForm
            .newBuilder()
            .withGrantTypeProperty("grant_type")
            .withGrantType("client_credentials")
            .withScope("test")
            .build()
            .toBodyString()).isEqualTo("grant_type=client_credentials&scope=test");
    }

    @Test
    void buildShouldThrowAnErrorWithoutClientIdProperty() {
        assertThatExceptionOfType(NullPointerException.class)
            .isThrownBy(() -> OAuth2AccessTokenRequestForm
                .newBuilder()
                .withGrantTypeProperty("grant_type")
                .withGrantType("client_credentials")
                .withClientSecretProperty("client_secret")
                .build())
            .withMessage("The client id property is required");
    }

    @Test
    void buildShouldThrowAnErrorWithoutClientSecretProperty() {
        assertThatExceptionOfType(NullPointerException.class)
            .isThrownBy(() -> OAuth2AccessTokenRequestForm
                .newBuilder()
                .withGrantTypeProperty("grant_type")
                .withGrantType("client_credentials")
                .withClientIdProperty("client_id")
                .build())
            .withMessage("The client secret property is required");
    }

    @Test
    void toBodyStringShouldThrowAnErrorWithoutClientIdValue() {
        assertThatExceptionOfType(NullPointerException.class)
            .isThrownBy(() -> OAuth2AccessTokenRequestForm
                .newBuilder()
                .withGrantTypeProperty("grant_type")
                .withGrantType("client_credentials")
                .withClientIdProperty("client_id")
                .withClientSecretProperty("client_secret")
                .withClientSecret("password")
                .build()
                .toBodyString())
            .withMessage("The client id is required");
    }

    @Test
    void toBodyStringShouldThrowAnErrorWithoutClientSecretValue() {
        assertThatExceptionOfType(NullPointerException.class)
            .isThrownBy(() -> OAuth2AccessTokenRequestForm
                .newBuilder()
                .withGrantTypeProperty("grant_type")
                .withGrantType("client_credentials")
                .withClientIdProperty("client_id")
                .withClientSecretProperty("client_secret")
                .withClientId("client")
                .build()
                .toBodyString())
            .withMessage("The client secret is required");
    }

    @Test
    void toBodyStringShouldBuildAValidForm() {
        assertThat(OAuth2AccessTokenRequestForm
            .newBuilder()
            .withGrantTypeProperty("grant_type")
            .withGrantType("client_credentials")
            .withClientIdProperty("client_id")
            .withClientId("id")
            .withClientSecretProperty("client_secret")
            .withClientSecret("password")
            .withScope("test")
            .build()
            .toBodyString()).isEqualTo("grant_type=client_credentials&scope=test&client_id=id&client_secret=password");
    }

    @Test
    void toBodyStringShouldBeIdempotent() {
        final OAuth2AccessTokenRequestForm form = OAuth2AccessTokenRequestForm
            .newBuilder()
            .withGrantTypeProperty("grant_type")
            .withGrantType("client_credentials")
            .withClientIdProperty("client_id")
            .withClientId("id")
            .withClientSecretProperty("client_secret")
            .withClientSecret("password")
            .withScope("test")
            .build();
        assertThat(form.toBodyString()).isEqualTo(form.toBodyString());
    }

}
