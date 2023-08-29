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
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.anyString;

abstract class HttpSenderTestBase<T extends AbstractHttpSender> {

    @Mock
    protected HttpClient mockedClient;

    @Mock
    protected HttpResponse<String> mockedResponse;

    protected static MockedStatic<BodyPublishers> bodyPublishers;

    @BeforeAll
    public static void setup() {
        final BodyPublisher bodyPublisher = Mockito.mock(BodyPublisher.class);
        bodyPublishers = Mockito.mockStatic(HttpRequest.BodyPublishers.class);
        bodyPublishers
            .when(() -> HttpRequest.BodyPublishers.ofString(anyString()))
            .thenReturn(bodyPublisher);
    }

    @AfterEach
    public void clear() {
        bodyPublishers.clearInvocations();
    }

    @AfterAll
    public static void clean() {
        bodyPublishers.close();
    }

}
