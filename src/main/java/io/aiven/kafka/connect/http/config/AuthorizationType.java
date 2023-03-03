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

public enum AuthorizationType {
    NONE("none"),
    OAUTH2("oauth2"),
    STATIC("static"),
    APIKEY("apikey");

    public final String name;

    AuthorizationType(final String name) {
        this.name = name;
    }

    public static AuthorizationType forName(final String name) {
        Objects.requireNonNull(name);

        if (NONE.name.equalsIgnoreCase(name)) {
            return NONE;
        } else if (OAUTH2.name.equalsIgnoreCase(name)) {
            return OAUTH2;
        } else if (STATIC.name.equalsIgnoreCase(name)) {
            return STATIC;
        } else if (APIKEY.name.equalsIgnoreCase(name)) {
            return APIKEY;
        } else {
            throw new IllegalArgumentException("Unknown authorization type: " + name);
        }
    }

    public static final Collection<String> NAMES =
            Arrays.stream(values()).map(v -> v.name).collect(Collectors.toList());
}
