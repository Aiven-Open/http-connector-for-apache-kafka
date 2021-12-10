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

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;

class JsonRecordValueConverter implements RecordValueConverter.Converter {

    private final JsonConverter jsonConverter;

    public JsonRecordValueConverter() {
        this.jsonConverter = new JsonConverter();
        jsonConverter.configure(Map.of("schemas.enable", false, "converter.type", "value"));
    }

    @Override
    public String convert(final SinkRecord record) {
        return new String(
                jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value()),
                StandardCharsets.UTF_8);
    }

}
