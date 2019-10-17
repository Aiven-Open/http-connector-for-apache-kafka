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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RecordQueueTest {
    @Test
    void testSimpleAdd() throws InterruptedException {
        final int recordsPerRun = 100;
        final RecordQueue queue = new RecordQueue(recordsPerRun);

        // Run 1

        List<SinkRecord> records = IntStream.range(0,  recordsPerRun)
            .mapToObj(i -> record("value-" + i, i))
            .collect(Collectors.toList());
        for (final SinkRecord record : records) {
            assertTrue(queue.offer(record));
        }

        assertFalse(queue.offer(record("value-" + recordsPerRun, recordsPerRun)));

        for (final SinkRecord record : records) {
            assertSame(record, queue.poll(1, TimeUnit.MILLISECONDS));
        }

        assertNull(queue.poll(1, TimeUnit.MILLISECONDS));

        // Run 2

        records = IntStream.range(recordsPerRun,  recordsPerRun * 2)
            .mapToObj(i -> record("value-" + i, i))
            .collect(Collectors.toList());
        for (final SinkRecord record : records) {
            assertTrue(queue.offer(record));
        }

        assertFalse(queue.offer(record("value-" + (recordsPerRun * 2), recordsPerRun * 2)));

        for (final SinkRecord record : records) {
            assertSame(record, queue.poll(1, TimeUnit.MILLISECONDS));
        }

        assertNull(queue.poll(1, TimeUnit.MILLISECONDS));
    }

    @Test
    void testWillNotAcceptLessOrEqualToMaxOffset() throws InterruptedException {
        final RecordQueue queue = new RecordQueue(5);
        final SinkRecord r0 = record("value-0", 0);
        final SinkRecord r1 = record("value-1", 1);
        final SinkRecord r2 = record("value-2", 2);
        assertTrue(queue.offer(r1));
        assertSame(r1, queue.poll(1, TimeUnit.MILLISECONDS));

        assertTrue(queue.offer(r0));
        assertNull(queue.poll(1, TimeUnit.MILLISECONDS));

        assertTrue(queue.offer(r1));
        assertNull(queue.poll(1, TimeUnit.MILLISECONDS));

        assertTrue(queue.offer(r2));
        assertSame(r2, queue.poll(1, TimeUnit.MILLISECONDS));
    }

    private SinkRecord record(final String value, final long offset) {
        return new SinkRecord(
            "the-topic",
            0,
            null, value,
            null, null,
            offset
        );
    }
}
