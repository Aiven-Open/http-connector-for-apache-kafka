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

package io.aiven.kafka.connect.http.converter;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

public class RecordValueConverter {
    private static final ConcurrentHashMap<Class<?>, Converter> RUNTIME_CLASS_TO_CONVERTER_CACHE =
        new ConcurrentHashMap<>();

    private final Map<Class<?>, Converter> converters;

    public static RecordValueConverter create(final HttpSinkConfig config) {
        RUNTIME_CLASS_TO_CONVERTER_CACHE.clear(); // Avoid state being preserved on task restarts
        final DecimalFormat decimalFormat = config.decimalFormat();
        final String converterType  =  config.converterType();
        RecordValueConverter.Converter jsonRecordValueConverter = null;
                switch (converterType) {
                case "device":
                        jsonRecordValueConverter = new PostBackConverter("/tmp", decimalFormat);
                        break;
                case "player":
                        jsonRecordValueConverter = new PostBackConverter("/tmp", decimalFormat);
                        break;
               default :
                        jsonRecordValueConverter = new JsonRecordValueConverter(decimalFormat);
                        break;
        }
        final Map<Class<?>, RecordValueConverter.Converter> converters =
                Map.of(
                String.class, record -> (String) record.value(),
                Map.class, jsonRecordValueConverter,
                Struct.class, jsonRecordValueConverter
        );

        return new RecordValueConverter(converters);
    }

    private RecordValueConverter(final Map<Class<?>, Converter> converters) {
        this.converters = converters;
    }

    public interface Converter {
        String convert(final SinkRecord record);
    }

    public String convert(final SinkRecord record) {
        final Converter converter = getConverter(record);
        return converter.convert(record);
    }

    private Converter getConverter(final SinkRecord record) {
        return RUNTIME_CLASS_TO_CONVERTER_CACHE.computeIfAbsent(record.value().getClass(), clazz -> {
            final boolean directlyConvertible = converters.containsKey(clazz);
            final List<Class<?>> convertibleByImplementedTypes = getAllSerializableImplementedInterfaces(clazz);
            validateConvertibility(clazz, directlyConvertible, convertibleByImplementedTypes);

            Class<?> implementedClazz = clazz;
            if (!directlyConvertible) {
                implementedClazz = convertibleByImplementedTypes.get(0);
            }
            return converters.get(implementedClazz);
        });
    }

    private List<Class<?>> getAllSerializableImplementedInterfaces(final Class<?> recordClazz) {
        // caching the computation since querying implemented interfaces is expensive.
        // The size of the cache is unlimited, but I don't think it's a problem
        // since the number of different record classes is limited.
        return getAllInterfaces(recordClazz).stream()
            .filter(converters::containsKey)
            .collect(Collectors.toList());
    }

    public static Set<Class<?>> getAllInterfaces(final Class<?> clazz) {
        final Set<Class<?>> interfaces = new HashSet<>();

        for (final Class<?> implementation : clazz.getInterfaces()) {
            interfaces.add(implementation);
            interfaces.addAll(getAllInterfaces(implementation));
        }

        if (clazz.getSuperclass() != null) {
            interfaces.addAll(getAllInterfaces(clazz.getSuperclass()));
        }

        return interfaces;
    }

    private static void validateConvertibility(
        final Class<?> recordClazz,
        final boolean directlyConvertible,
        final List<Class<?>> convertibleByImplementedTypes
    ) {
        final boolean isConvertibleType = directlyConvertible || !convertibleByImplementedTypes.isEmpty();

        if (!isConvertibleType) {
            throw new DataException(
                String.format(
                    "Record value must be a String, a Schema Struct or implement "
                        + "`java.util.Map`, but %s is given",
                    recordClazz));
        }
        if (!directlyConvertible && convertibleByImplementedTypes.size() > 1) {
            final String implementedTypes = convertibleByImplementedTypes.stream().map(Class::getSimpleName)
                .collect(Collectors.joining(", ", "[", "]"));
            throw new DataException(
                String.format(
                    "Record value must be only one of String, Schema Struct or implement "
                        + "`java.util.Map`, but %s matches multiple types: %s",
                    recordClazz, implementedTypes));
        }
    }
}
