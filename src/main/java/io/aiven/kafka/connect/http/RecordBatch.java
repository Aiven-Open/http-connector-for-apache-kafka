/*
 * Copyright 2019 Aiven Oy
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

package io.aiven.kafka.connect.http;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

final class RecordBatch {
    private final List<SinkRecord> records;

    RecordBatch(final Collection<SinkRecord> records) {
        this.records = List.copyOf(records);
    }

    List<SinkRecord> records() {
        return records;
    }

    Map<TopicPartition, OffsetAndMetadata> latestOffsets() {
        final Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
        for (final SinkRecord record : records) {
            final TopicPartition topicPartition =
                new TopicPartition(record.topic(), record.kafkaPartition());
            if (!result.containsKey(topicPartition)
                || result.get(topicPartition).offset() < record.kafkaOffset()
            ) {
                result.put(topicPartition, new OffsetAndMetadata(record.kafkaOffset()));
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return "RecordBatch("
            + "records=" + records
            + ")";
    }
}
