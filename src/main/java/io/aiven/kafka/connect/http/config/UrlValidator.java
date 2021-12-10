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

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

class UrlValidator implements ConfigDef.Validator {

    private final boolean skipNullString;

    UrlValidator() {
        this(false);
    }

    UrlValidator(final boolean skipNullString) {
        this.skipNullString = skipNullString;
    }

    @Override
    public void ensureValid(final String name, final Object value) {
        if (skipNullString && value == null) {
            return;
        }
        if (value == null) {
            throw new ConfigException(name, null, "can't be null");
        }
        if (!(value instanceof String)) {
            throw new ConfigException(name, value, "must be string");
        }
        try {
            new URL((String) value);
        } catch (final MalformedURLException e) {
            throw new ConfigException(name, value, "malformed URL");
        }
    }

    @Override
    public String toString() {
        return "HTTP(S) URL";
    }

}
