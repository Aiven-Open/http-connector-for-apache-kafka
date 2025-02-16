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

import javax.swing.UIDefaults;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;


import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class RecordValueConverterTest {

    @Mock
    private HttpSinkConfig httpSinkConfig;

    private RecordValueConverter recordValueConverter;

    @BeforeEach
    void setup() {
        when(httpSinkConfig.decimalFormat()).thenReturn(DecimalFormat.NUMERIC);
        this.recordValueConverter = RecordValueConverter.create(httpSinkConfig);
    }

    @Test
    void convertAvroRecord() {
        final var recordSchema = SchemaBuilder.struct()
                .name("record")
                .field("name", SchemaBuilder.string())
                .field("value", SchemaBuilder.string());

        final var value = new Struct(recordSchema);
        value.put(new Field("name", 0, SchemaBuilder.string()), "user-0");
        value.put(new Field("value", 1, SchemaBuilder.string()), "value-0");

        final var sinkRecord = new SinkRecord(
                "some-topic", 0,
                SchemaBuilder.string(),
                "some-key", recordSchema, value, 1L);

        assertThat(recordValueConverter.convert(sinkRecord))
                .isEqualTo("{\"name\":\"user-0\",\"value\":\"value-0\"}");
    }

    @Test
    void convertStringRecord() {
        final var recordSchema = SchemaBuilder.string();

        final var sinkRecord = new SinkRecord(
                "some-topic", 0,
                SchemaBuilder.string(),
                "some-key", recordSchema, "some-str-value", 1L);

        assertThat(recordValueConverter.convert(sinkRecord)).isEqualTo("some-str-value");
    }

    @Test
    void convertWeirdMapRecord() {
        final var recordSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA);

        final UIDefaults value = new UIDefaults(
            new String[] {"Font", "BeautifulFont"}
        );

        final var sinkRecord = new SinkRecord(
            "some-topic", 0,
            SchemaBuilder.string(),
            "some-key", recordSchema, value, 1L);

        assertThat(recordValueConverter.convert(sinkRecord)).isEqualTo("{\"Font\":\"BeautifulFont\"}");
    }

    @Test
    void convertHashMapRecord() {
        final var recordSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA);

        final Map<String, String> value = new HashMap<>();
        value.put("key", "value");

        final var sinkRecord = new SinkRecord(
                "some-topic", 0,
                SchemaBuilder.string(),
                "some-key", recordSchema, value, 1L);

        assertThat(recordValueConverter.convert(sinkRecord)).isEqualTo("{\"key\":\"value\"}");
    }

    @Test
    void convertLinkedHashMapRecord() {
        final var recordSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA);

        final Map<String, String> value = new LinkedHashMap<>();
        value.put("key", "value");

        final var sinkRecord = new SinkRecord(
                "some-topic", 0,
                SchemaBuilder.string(),
                "some-key", recordSchema, value, 1L);

        assertThat(recordValueConverter.convert(sinkRecord)).isEqualTo("{\"key\":\"value\"}");
    }


    @Test
    void throwsDataExceptionForUnknownRecordValueClass() {
        final var recordSchema = SchemaBuilder.int64();
        final var sinkRecord = new SinkRecord(
                "some-topic", 0,
                SchemaBuilder.string(), "some-key",
                recordSchema, 42L, 1L);

        assertThatExceptionOfType(DataException.class)
                .isThrownBy(() -> recordValueConverter.convert(sinkRecord)).isInstanceOf(DataException.class);
    }

}
