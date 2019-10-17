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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.connect.sink.SinkRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RecordSender {
    private static final Logger log = LoggerFactory.getLogger(RecordSender.class);

    private final HttpSender httpSender;

    private final int maxRetries;
    private final int retryBackoffMs;

    private final RecordQueue outstandingRecords;
    private volatile Exception sendException = null;
    private final Map<TopicPartition, OffsetAndMetadata> lastSentOffsets = new CopyOnWriteMap<>();

    private final ExecutorService executorService;

    RecordSender(final HttpSender httpSender,
                 final int maxOutstandingRecords,
                 final int maxRetries, final int retryBackoffMs) {
        this.httpSender = httpSender;

        this.outstandingRecords = new RecordQueue(maxOutstandingRecords);

        this.maxRetries = maxRetries;
        this.retryBackoffMs = retryBackoffMs;

        this.executorService = Executors.newSingleThreadExecutor();
        this.executorService.submit(new BackgroundSender());
    }

    boolean send(final SinkRecord record) {
        return outstandingRecords.offer(record);
    }

    Exception sendException() {
        return sendException;
    }

    Map<TopicPartition, OffsetAndMetadata> lastSentOffsets() {
        return lastSentOffsets;
    }

    void stop() {
        executorService.shutdownNow();
    }

    private final class BackgroundSender implements Runnable {
        @Override
        public void run() {
            try {
                while (true) {
                    final SinkRecord record = outstandingRecords.poll(1, TimeUnit.SECONDS);
                    if (record == null) {
                        continue;
                    }

                    // TODO proper batching
                    final RecordBatch batch = new RecordBatch(List.of(record));
                    if (!sendBatchWithRetries(batch)) {
                        return;
                    }

                    lastSentOffsets.putAll(batch.latestOffsets());
                }
            } catch (final InterruptedException e) {
                log.info("Interrupted", e);
            }
        }

        private boolean sendBatchWithRetries(final RecordBatch batch) throws InterruptedException {
            log.debug("Sending batch {}", batch);

            // TODO proper batching
            final String body = (String) batch.records().get(0).value();

            int remainRetries = RecordSender.this.maxRetries;
            while (true) {
                try {
                    httpSender.send(body);
                    return true;
                } catch (final IOException e) {
                    if (remainRetries > 0) {
                        log.info("Sending batch {} failed, will retry in {} ms ({} retries remain)",
                            batch, RecordSender.this.retryBackoffMs, remainRetries, e);
                        remainRetries -= 1;
                        Thread.sleep(RecordSender.this.retryBackoffMs);
                    } else {
                        log.error("Sending batch {} failed and no retries remain, stopping",
                            batch);
                        sendException = e;
                        return false;
                    }
                }
            }
        }
    }
}
