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
import java.util.Collection;
import java.util.List;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RecordSender {
    private static final Logger log = LoggerFactory.getLogger(RecordSender.class);

    private final HttpSender httpSender;

    private final int maxRetries;
    private final int retryBackoffMs;

    RecordSender(final HttpSender httpSender,
                 final int maxRetries, final int retryBackoffMs) {
        this.httpSender = httpSender;

        this.maxRetries = maxRetries;
        this.retryBackoffMs = retryBackoffMs;
    }

    void send(final Collection<SinkRecord> records) throws InterruptedException {
        for (final SinkRecord record : records) {
            // TODO proper batching
            final RecordBatch batch = new RecordBatch(List.of(record));
            sendBatchWithRetries(batch);
        }
    }

    private void sendBatchWithRetries(final RecordBatch batch) throws InterruptedException {
        // TODO proper batching
        final String body = (String) batch.records().get(0).value();

        int remainRetries = RecordSender.this.maxRetries;
        while (true) {
            try {
                httpSender.send(body);
                return;
            } catch (final IOException e) {
                if (remainRetries > 0) {
                    log.info("Sending batch failed, will retry in {} ms ({} retries remain)",
                        RecordSender.this.retryBackoffMs, remainRetries, e);
                    remainRetries -= 1;
                    Thread.sleep(RecordSender.this.retryBackoffMs);
                } else {
                    log.error("Sending batch failed and no retries remain, stopping");
                    throw new ConnectException(e);
                }
            }
        }
    }
}
