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

import java.util.HashSet;
import java.util.List;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;


public class KeyValuePairListValidator implements ConfigDef.Validator {
    private final String delimiter;

    public KeyValuePairListValidator(final String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public void ensureValid(final String name, final Object value) {
        if (!(value instanceof List)) {
            throw new ConfigException(name, value, "must be a list with the specified format");
        }
        @SuppressWarnings("unchecked")
        final var values = (List<String>) value;
        final var keySet = new HashSet<String>();
        for (final String headerField : values) {
            final var splitHeaderField = headerField.split(delimiter, -1);
            final String lowerCaseKey = splitHeaderField[0].toLowerCase();
            if (splitHeaderField.length != 2) {
                throw new ConfigException("Header field should use format header:value");
            }
            if (keySet.contains(lowerCaseKey)) {
                throw new ConfigException("Duplicate keys are not allowed (case-insensitive)");
            }
            keySet.add(lowerCaseKey);
        }
    }

    @Override
    public String toString() {
        return "Key value pair string list with format header:value";
    }
}
