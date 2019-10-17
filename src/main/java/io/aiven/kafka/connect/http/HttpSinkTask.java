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
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HttpSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(HttpSinkTask.class);

    private static final RetriableException CANNOT_ACCEPT_RECORDS_EXCEPTION =
        new RetriableException("Cannot accept records, some previous records are being sent");

    private HttpSinkConfig config;

    private HttpSender httpSender;
    private RecordSender recordSender;

    // required by Connect
    public HttpSinkTask() {
    }

    // for testing
    HttpSinkTask(final HttpSender httpSender) {
        this.httpSender = httpSender;
    }

    @Override
    public void start(final Map<String, String> props) {
        Objects.requireNonNull(props);

        this.config = new HttpSinkConfig(props);

        if (this.httpSender == null) {
            this.httpSender = new HttpSender(
                config.httpUrl(),
                config.headerAuthorization(),
                config.headerContentType());
        }

        recordSender = new RecordSender(httpSender,
            config.maxOutstandingRecords(),
            config.maxRetries(), config.retryBackoffMs());
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        log.debug("Received {} records", records.size());

        if (recordSender.sendException() != null) {
            throw new ConnectException(recordSender.sendException());
        }
//        context.timeout(1000);

        if (!records.isEmpty()) {
            int recordsAccepted = 0;
            try {
                for (final SinkRecord record : records) {
                    if (record.value() == null) {
                        throw new DataException("Record value must not be null");
                    }
                    if (!(record.value() instanceof String)) {
                        throw new DataException(
                            "Record value must be String, but " + record.getClass() + " + is given");
                    }

                    final boolean sendResult = recordSender.send(record);
                    if (sendResult) {
                        recordsAccepted += 1;
                    } else {
                        throw CANNOT_ACCEPT_RECORDS_EXCEPTION;
                    }
                }
            } finally {
                // When debugging, don't forget that another thread is consuming records in the background.
                log.debug("Accepted records: {} / {}", recordsAccepted, records.size());
            }
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
        final Map<TopicPartition, OffsetAndMetadata> currentOffsets
    ) {
        return recordSender.lastSentOffsets();
    }

    @Override
    public void stop() {
        if (recordSender != null) {
            recordSender.stop();
        }
    }

    @Override
    public String version() {
        return Version.VERSION;
    }
}
