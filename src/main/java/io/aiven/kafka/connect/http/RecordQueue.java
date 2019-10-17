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

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A queue for {@link SinkRecord}s, which preserves ascending order of offsets
 * per topic-partition. I.e. if a record with some X offset was added, the queue
 * will not add any messages with offsets lower or equal to X.
 */
final class RecordQueue {
    private static final Logger log = LoggerFactory.getLogger(RecordQueue.class);

    private final ArrayBlockingQueue<SinkRecord> records;
    private final Map<TopicPartition, Long> highetsAcceptedOffsets = new ConcurrentHashMap<>();

    RecordQueue(final int capacity) {
        this.records = new ArrayBlockingQueue<>(capacity);
    }

    /**
     * Add the record to the queue.
     * @return {@code true} if the record has just been accepted,
     *         <b>or</b> it's offset is not greater than the highest previously accepted
     *         offset for this topic-partition.
     *         Otherwise, {@code false}.
     */
    boolean offer(final SinkRecord record) {
        final TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
        final long lastAcceptedOffset = highetsAcceptedOffsets.getOrDefault(topicPartition, -1L);
        if (record.kafkaOffset() <= lastAcceptedOffset) {
            log.error("ALARM {} {}", record, lastAcceptedOffset);
            return true;
        }

        final boolean offerResult = records.offer(record);
        if (offerResult) {
            highetsAcceptedOffsets.put(
                topicPartition,
                record.kafkaOffset()
            );
        }
        return offerResult;
    }

    SinkRecord poll(final long timeout, final TimeUnit unit) throws InterruptedException {
        return records.poll(timeout, unit);
    }
}
