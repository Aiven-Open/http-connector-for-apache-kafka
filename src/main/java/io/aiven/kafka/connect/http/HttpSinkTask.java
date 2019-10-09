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

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HttpSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(HttpSinkTask.class);

    private HttpSinkConfig config;

    private HttpSender httpSender;

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
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        log.debug("Received {} records", records.size());

        for (final SinkRecord record : records) {
            if (record.value() == null) {
                throw new DataException("Record value must not be null");
            }
            if (!(record.value() instanceof String)) {
                throw new DataException("Record value must be String, but " + record.getClass() + " + is given");
            }

            // TODO add proper batching
            httpSender.sendBatch((String) record.value());
        }
    }

    @Override
    public void stop() {
        // Nothing to do.
    }

    @Override
    public String version() {
        return Version.VERSION;
    }
}
