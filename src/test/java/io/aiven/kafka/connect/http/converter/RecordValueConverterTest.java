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

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


import org.apache.kafka.connect.data.Decimal;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
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
        final var decimalFieldSchema = Decimal.builder(2).optional().build();
        final var recordSchema = SchemaBuilder.struct()
                .name("record")
                .field("name", SchemaBuilder.string())
                .field("value", SchemaBuilder.string())
                .field("price", decimalFieldSchema);

        final var value = new Struct(recordSchema);
        value.put(new Field("name", 0, SchemaBuilder.string()), "user-0");
        value.put(new Field("value", 1, SchemaBuilder.string()), "value-0");
        value.put(new Field("price", 2, decimalFieldSchema), new BigDecimal("34.99"));

        final var sinkRecord = new SinkRecord(
                "some-topic", 0,
                SchemaBuilder.string(),
                "some-key", recordSchema, value, 1L);

        assertThat(recordValueConverter.convert(sinkRecord))
                .isEqualTo("{\"name\":\"user-0\",\"value\":\"value-0\",\"price\":34.99}");
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

    @ParameterizedTest
    @CsvSource({
            "NUMERIC , 150.75 , 150.75",
            "BASE64  , 99.99  , \"Jw8=\""
    })
    void convertAvroRecordWithDecimals(
            DecimalFormat format, String decimalValue, String finalSnippet) {
        when(httpSinkConfig.decimalFormat()).thenReturn(format);
        recordValueConverter = RecordValueConverter.create(httpSinkConfig);

        final var decimalSchema = Decimal.builder(2).build();
        final var schema = SchemaBuilder.struct().field("value", decimalSchema);
        final var value = new Struct(schema).put("value", new BigDecimal(decimalValue));

        final var sinkRecord = createSinkRecord(schema, value);

        final var expectedJson = "{\"value\":" + finalSnippet + "}";
        assertThat(recordValueConverter.convert(sinkRecord)).isEqualTo(expectedJson);
    }

    @ParameterizedTest
    @CsvSource({
            "NUMERIC , 12.34 , 1.234, 12.34 , 1.234",
            "BASE64  , 12.34 , 1.234 , \"BNI=\", \"BNI=\""
    })
    void convertAvroRecordWithNestedDecimals(
            final DecimalFormat format,
            final String parentValue,
            final String childValue,
            final String expectedParentValue,
            final String expectedChildValue) {
        when(httpSinkConfig.decimalFormat()).thenReturn(format);
        recordValueConverter = RecordValueConverter.create(httpSinkConfig);

        final var parentSchema = Decimal.builder(2).build();
        final var childSchema = Decimal.builder(3).build();

        final var schema = SchemaBuilder.struct()
                .field("value", parentSchema)
                .field("child", SchemaBuilder.struct()
                        .field("nestedValue", childSchema).build());

        final var value = new Struct(schema).put("value", new BigDecimal(parentValue));
        final var childValueStruct = new Struct(schema.field("child").schema())
                .put("nestedValue", new BigDecimal(childValue));
        value.put("child", childValueStruct);

        final var sinkRecord = createSinkRecord(schema, value);

        final var expectedJson = String.format("{\"value\":%s,\"child\":{\"nestedValue\":%s}}", expectedParentValue, expectedChildValue);
        assertThat(recordValueConverter.convert(sinkRecord)).isEqualTo(expectedJson);
    }

    @ParameterizedTest
    @CsvSource(value = {
            "NUMERIC | 2 | 12.34 | 45.67 | [12.34,45.67]",
            "BASE64  | 2 | 12.34 | 45.67 | [\"BNI=\",\"Edc=\"]",
            "NUMERIC | 2 | 99.99 | 0.01  | [99.99,0.01]",
            "BASE64  | 2 | 99.99 | 0.01  | [\"Jw8=\",\"AQ==\"]"
    }, delimiterString = "|")
    void convertAvroRecordWithDecimalArray(
            final DecimalFormat format,
            final int scale,
            final String val1,
            final String val2,
            final String expectedJsonArrayValue) {
        when(httpSinkConfig.decimalFormat()).thenReturn(format);
        recordValueConverter = RecordValueConverter.create(httpSinkConfig);

        final var decimalSchema = Decimal.builder(scale).build();
        final var schema = SchemaBuilder.struct().field("values", SchemaBuilder.array(decimalSchema));
        final var value = new Struct(schema).put("values", List.of(new BigDecimal(val1), new BigDecimal(val2)));

        final var sinkRecord = createSinkRecord(schema, value);

        final var expectedJson = "{\"values\":" + expectedJsonArrayValue + "}";
        assertThat(recordValueConverter.convert(sinkRecord)).isEqualTo(expectedJson);
    }

    @ParameterizedTest
    @CsvSource({
            "NUMERIC , 2 , 12.34      , 12.34",
            "NUMERIC , 3 , 1.234      , 1.234",
            "NUMERIC , 2 , 123456.78  , 123456.78",
            "NUMERIC , 5 , 123.45678  , 123.45678",
            // BASE64
            "BASE64  , 2 , 12.34      , \"BNI=\"",
            "BASE64  , 3 , 1.234      , \"BNI=\"",
            "BASE64  , 2 , 123456.78  , \"ALxhTg==\"",
            "BASE64  , 5 , 123.45678  , \"ALxhTg==\"",
    })
    void convertAvroRecordWithDecimalHavingMultipleScales(final DecimalFormat format,
                                          final int scale,
                                          final String actualValue,
                                          final String expectedValue) {
        when(httpSinkConfig.decimalFormat()).thenReturn(format);
        recordValueConverter = RecordValueConverter.create(httpSinkConfig);

        final var decimalSchema = Decimal.builder(scale).build();
        final var schema = SchemaBuilder.struct().field("value", decimalSchema);

        final var value = new Struct(schema)
                .put("value", new BigDecimal(actualValue));

        final var sinkRecord = createSinkRecord(schema, value);

        final var expectedJson = "{\"value\":" + expectedValue + "}";
        assertThat(recordValueConverter.convert(sinkRecord))
                .isEqualTo(expectedJson);
    }

    @ParameterizedTest
    @EnumSource(DecimalFormat.class)
    void convertAvroRecordWithNullValues(DecimalFormat format) {
        when(httpSinkConfig.decimalFormat()).thenReturn(format);
        this.recordValueConverter = RecordValueConverter.create(httpSinkConfig);

        final var decimalSchema = Decimal.builder(2).optional().build();
        final var recordSchema = SchemaBuilder.struct()
                .name("record")
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("value", decimalSchema);

        final var value = new Struct(recordSchema);
        value.put("name", null);
        value.put("value", null);

        final var sinkRecord = new SinkRecord(
                "some-topic", 0,
                Schema.STRING_SCHEMA, "some-key",
                recordSchema, value, 1L);

        assertThat(recordValueConverter.convert(sinkRecord))
                .isEqualTo("{\"name\":null,\"value\":null}"); // Expected empty JSON object for null values
    }

    private static SinkRecord createSinkRecord(SchemaBuilder schema, Struct value) {
        return new SinkRecord("test-topic", 0, null, null, schema, value, 1L);
    }
}
