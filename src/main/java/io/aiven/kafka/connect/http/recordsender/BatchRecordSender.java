/*
 * Copyright 2019 Aiven Oy and http-connector-for-apache-kafka project contributors
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

package io.aiven.kafka.connect.http.recordsender;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.http.sender.HttpSender;

final class BatchRecordSender extends RecordSender {
    private final int batchMaxSize;
    private final String batchPrefix;
    private final String batchSuffix;
    private final String batchSeparator;

    protected BatchRecordSender(final HttpSender httpSender,
                                final int batchMaxSize,
                                final String batchPrefix,
                                final String batchSuffix,
                                final String batchSeparator) {
        super(httpSender);
        this.batchMaxSize = batchMaxSize;
        this.batchPrefix = batchPrefix;
        this.batchSuffix = batchSuffix;
        this.batchSeparator = batchSeparator;
    }

    @Override
    public void send(final Collection<SinkRecord> records) {
        final List<SinkRecord> batch = new ArrayList<>(batchMaxSize);
        for (final var record : records) {
            batch.add(record);
            if (batch.size() >= batchMaxSize) {
                final String body = createRequestBody(batch);
                batch.clear();
                httpSender.send(body);
            }
        }

        if (!batch.isEmpty()) {
            final String body = createRequestBody(batch);
            httpSender.send(body);
        }
    }

    @Override
    public void send(final SinkRecord record) {
        throw new ConnectException("Don't call this method for batch sending");
    }

    private String createRequestBody(final Collection<SinkRecord> batch) {
        final StringBuilder result = new StringBuilder();
        if (!batchPrefix.isEmpty()) {
            result.append(batchPrefix);
        }
        final Iterator<SinkRecord> it = batch.iterator();
        if (it.hasNext()) {
            result.append(recordValueConverter.convert(it.next()));
            while (it.hasNext()) {
                result.append(batchSeparator);
                result.append(recordValueConverter.convert(it.next()));
            }
        }
        if (!batchSuffix.isEmpty()) {
            result.append(batchSuffix);
        }
        return result.toString();
    }
}
