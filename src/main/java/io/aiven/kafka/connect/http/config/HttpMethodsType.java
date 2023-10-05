/*
 * Copyright 2019 Aiven Oy and http-connector-for-apache-kafka project contributors
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

package io.aiven.kafka.connect.http.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

public enum HttpMethodsType
{
    POST("POST"),
    PUT("PUT");

    public final String name;

    HttpMethodsType(final String name) {
        this.name = name;
    }

    public static HttpMethodsType forName(final String name) {
        Objects.requireNonNull(name);
        return Arrays.stream(values())
                .filter(v -> v.name.equalsIgnoreCase(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("HTTP Method type: " + name));
    }

    public static final Collection<String> NAMES =
            Arrays.stream(values()).map(v -> v.name).collect(Collectors.toList());
}
