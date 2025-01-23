package net.atos.kafka.connect.http.recordfilter;

import io.aiven.kafka.connect.http.converter.RecordValueConverter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class RecordValueFilterTest {

    final RecordValueConverter recordValueConverter = new RecordValueConverter();

    @Test
    void testFilterRecord() {
        final var recordSchema = SchemaBuilder.struct()
                .name("record")
                .field("name", SchemaBuilder.string())
                .field("value", SchemaBuilder.string());
        List<SinkRecord> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {

            final var value = new Struct(recordSchema);
            value.put(new Field("name", 0, SchemaBuilder.string()), "user-"+i);
            value.put(new Field("value", 1, SchemaBuilder.string()), "value-"+i);

            final var sinkRecord = new SinkRecord(
                    "some-topic", 0,
                    SchemaBuilder.string(),
                    "some-key-"+i, recordSchema, value, i);
            records.add(sinkRecord);
        }

        RecordFilter recordFilter = RecordFilter.createRecordSender(null,"@.name == 'user-1'");
        Collection<SinkRecord> listRecords = recordFilter.filter(records);
        assertThat(listRecords).extracting(recordValueConverter::convert).isNotEmpty()
                .allMatch(r ->  r.equals("{\"name\":\"user-1\",\"value\":\"value-1\"}"));
    }


}
