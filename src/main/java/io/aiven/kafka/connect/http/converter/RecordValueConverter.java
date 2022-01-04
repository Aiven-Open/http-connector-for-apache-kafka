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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

public class RecordValueConverter {

    private final JsonRecordValueConverter jsonRecordValueConverter = new JsonRecordValueConverter();

    private final Map<Class<?>, Converter> converters = Map.of(
            String.class, record -> (String) record.value(),
            HashMap.class, jsonRecordValueConverter,
            Struct.class, jsonRecordValueConverter
    );

    interface Converter {
        String convert(final SinkRecord record);
    }

    public String convert(final SinkRecord record) {
        if (!converters.containsKey(record.value().getClass())) {
            throw new DataException(
                    "Record value must be String, Schema Struct or HashMap," 
                    + " but " + record.value().getClass() + " is given");
        }
        return converters.get(record.value().getClass()).convert(record);
    }

}
