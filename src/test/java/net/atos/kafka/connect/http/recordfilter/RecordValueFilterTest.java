/*
 * Copyright 2025 Atos
 *
 */

package net.atos.kafka.connect.http.recordfilter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;
import io.aiven.kafka.connect.http.converter.RecordValueConverter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RecordValueFilterTest {

    private RecordValueConverter recordValueConverter;
    private HttpSinkConfig config;

    @BeforeEach
    void setup() {
        this.config = new HttpSinkConfig(defaultConfig());
        this.recordValueConverter = RecordValueConverter.create(this.config);
    }

    @Test
    void testFilterBasicRecord() {
        final var recordSchema = SchemaBuilder.struct()
                .name("record")
                .field("name", SchemaBuilder.string())
                .field("value", SchemaBuilder.string());

        final List<SinkRecord> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final var value = new Struct(recordSchema);
            value.put(new Field("name", 0, SchemaBuilder.string()), "user-" + i);
            value.put(new Field("value", 1, SchemaBuilder.string()), "value-" + i);

            final var sinkRecord = new SinkRecord(
                    "some-topic", 0,
                    SchemaBuilder.string(),
                    "some-key-" + i, recordSchema, value, i);
            records.add(sinkRecord);
        }

        final RecordFilter recordFilter = RecordFilter.createRecordFilter(this.config, "@.name == 'user-1'");
        final Collection<SinkRecord> listRecords = recordFilter.filter(records);
        assertThat(listRecords).extracting(recordValueConverter::convert).isNotEmpty()
                .allMatch(r -> r.equals("{\"name\":\"user-1\",\"value\":\"value-1\"}"));
    }

    @Test
    void testFilterRegexRecord() {
        final var recordSchema = SchemaBuilder.struct()
                .name("record")
                .field("name", SchemaBuilder.string())
                .field("value", SchemaBuilder.string());

        final List<SinkRecord> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final var value = new Struct(recordSchema);
            value.put(new Field("name", 0, SchemaBuilder.string()), "user-" + i);
            value.put(new Field("value", 1, SchemaBuilder.string()), "value-" + i);

            final var sinkRecord = new SinkRecord(
                    "some-topic", 0,
                    SchemaBuilder.string(),
                    "some-key-" + i, recordSchema, value, i);
            records.add(sinkRecord);
        }

        final RecordFilter recordFilter = RecordFilter.createRecordFilter(this.config, "@.name =~ /user-.*/");
        final Collection<SinkRecord> listRecords = recordFilter.filter(records);
        assertThat(listRecords).hasSize(10);
    }

    @Test
    void testFilterWrongJsonRecord() {
        final var recordSchema = SchemaBuilder.struct()
                .name("record")
                .field("name", SchemaBuilder.string())
                .field("value", SchemaBuilder.string());

        final List<SinkRecord> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final var value = new Struct(recordSchema);
            value.put(new Field("name", 0, SchemaBuilder.string()), "user-" + i);
            value.put(new Field("value", 1, SchemaBuilder.string()), "value-" + i);

            final var sinkRecord = new SinkRecord(
                    "some-topic", 0,
                    SchemaBuilder.string(),
                    "some-key-" + i, recordSchema, value, i);
            records.add(sinkRecord);
        }

        final RecordFilter recordFilter = RecordFilter.createRecordFilter(this.config, "@.name =~ /user-.*/");
        final Collection<SinkRecord> listRecords = recordFilter.filter(records);
        assertThat(listRecords).hasSize(10);
    }

    @Test
    void testFilterStringRecord() {
        final var recordSchema = SchemaBuilder.string();

        final var sinkRecord = new SinkRecord(
                "some-topic", 0,
                SchemaBuilder.string(),
                "name", recordSchema, "some-str-value", 1L);

        final RecordFilter recordFilter = RecordFilter.createRecordFilter(this.config, "@.name =~ /str/");
        final Collection<SinkRecord> listRecords = recordFilter.filter(List.of(sinkRecord));
        assertThat(listRecords).isEmpty();
    }

    @Test
    void testFilterNullRecord() {
        final var recordSchema = SchemaBuilder.string();

        final var sinkRecord = new SinkRecord(
                "some-topic", 0,
                SchemaBuilder.string(),
                "name", recordSchema, null, 1L);

        final RecordFilter recordFilter = RecordFilter.createRecordFilter(this.config, "@.name =~ /str/");
        final Collection<SinkRecord> listRecords = recordFilter.filter(List.of(sinkRecord));
        assertThat(listRecords).isEmpty();
    }

    private Map<String, String> defaultConfig() {
        return Map.of("http.url", "http://localhost:42", "http.authorization.type", "static",
                "http.headers.authorization", "Bearer myToken");
    }
}
