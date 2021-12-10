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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.ConfigDef;

/**
 * A {@link ConfigDef.Recommender} that always supports only
 * the predefined set of values. {@link #visible(String, Map)} is always {@code true}.
 */
class FixedSetRecommender implements ConfigDef.Recommender {

    private final List<Object> supportedValues;

    private FixedSetRecommender(final Collection<?> supportedValues) {
        Objects.requireNonNull(supportedValues);
        this.supportedValues = new ArrayList<>(supportedValues);
    }

    @Override
    public List<Object> validValues(final String name, final Map<String, Object> parsedConfig) {
        return Collections.unmodifiableList(supportedValues);
    }

    @Override
    public boolean visible(final String name, final Map<String, Object> parsedConfig) {
        return true;
    }

    static FixedSetRecommender ofSupportedValues(final Collection<?> supportedValues) {
        Objects.requireNonNull(supportedValues);
        return new FixedSetRecommender(supportedValues);
    }
}
