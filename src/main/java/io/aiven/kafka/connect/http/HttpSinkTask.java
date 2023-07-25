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

package io.aiven.kafka.connect.http;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import io.aiven.kafka.connect.http.config.BehaviorOnNullValue;
import io.aiven.kafka.connect.http.config.HttpSinkConfig;
import io.aiven.kafka.connect.http.recordsender.RecordSender;
import io.aiven.kafka.connect.http.sender.HttpSender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HttpSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(HttpSinkTask.class);

    private HttpSender httpSender;
    private RecordSender recordSender;
    private ErrantRecordReporter reporter;

    private BehaviorOnNullValue behaviorOnNullValue;

    // flag for legacy support (configured for batch mode, not using errors.tolerance)
    private boolean useBatchSend;

    // required by Connect
    public HttpSinkTask() {
        this(null);
    }

    HttpSinkTask(final HttpSender httpSender) {
        this.httpSender = httpSender;
    }

    @Override
    public void start(final Map<String, String> props) {
        Objects.requireNonNull(props);
        final var config = new HttpSinkConfig(props);
        if (this.httpSender == null) {
            this.httpSender = HttpSender.createHttpSender(config);
        }

        recordSender = RecordSender.createRecordSender(httpSender, config);

        this.useBatchSend = config.batchingEnabled();
        this.behaviorOnNullValue = config.behaviorOnNullValues();

        if (Objects.nonNull(config.kafkaRetryBackoffMs())) {
            context.timeout(config.kafkaRetryBackoffMs());
        }

        try {
            if (context.errantRecordReporter() == null) {
                log.info("Errant record reporter not configured.");
            }

            // may be null if DLQ not enabled
            reporter = context.errantRecordReporter();
        } catch (NoClassDefFoundError | NoSuchMethodError e) {
            // Will occur in Connect runtimes earlier than 2.6
            log.warn("Apache Kafka versions prior to 2.6 do not support the errant record reporter.");
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        log.debug("Received {} records", records.size());

        if (records.isEmpty()) {
            return;
        }

        records.stream().filter(HttpSinkTask::isTombstone).findFirst().ifPresent(r -> {
            if (Objects.requireNonNull(behaviorOnNullValue) == BehaviorOnNullValue.LOG) {
                log.warn("Record value was null at offset: {} partition: {}", r.kafkaOffset(), r.kafkaPartition());
            } else if (behaviorOnNullValue == BehaviorOnNullValue.FAIL) {
                throw new DataException("Record value must not be null at offset: " + r.kafkaOffset()
                        + " and partition: " + r.kafkaPartition());
            }
        });

        final List<SinkRecord> recordsToSend = records.stream()
                .filter(r -> !isTombstone(r))
                .collect(Collectors.toList());
        // use the legacy send if batch is enabled
        if (this.useBatchSend) {
            recordSender.send(recordsToSend);
        } else {
            // send records to the sender one at a time
            for (final SinkRecord r : recordsToSend) {
                singleSend(r);
            }
        }
    }

    private void singleSend(final SinkRecord r) {
        try {
            recordSender.send(r);
        } catch (final ConnectException e) {
            if (reporter != null) {
                reporter.report(r, e);
            } else {
                // otherwise, re-throw the exception
                throw new ConnectException(e.getMessage());
            }
        }
    }

    private static boolean isTombstone(final SinkRecord r) {
        return r.value() == null;
    }

    @Override
    public void stop() {
        // do nothing
    }

    @Override
    public String version() {
        return Version.VERSION;
    }

}
