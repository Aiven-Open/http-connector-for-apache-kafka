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

package io.aiven.kafka.connect.http.recordsender;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.http.sender.HttpSender;

final class BatchRecordSender extends RecordSender {
    private static final String BATCH_RECORD_SEPARATOR = "\n";

    private final int batchMaxSize;

    protected BatchRecordSender(final HttpSender httpSender, final int batchMaxSize) {
        super(httpSender);
        this.batchMaxSize = batchMaxSize;
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

    private String createRequestBody(final Collection<SinkRecord> batch) {
        final StringBuilder result = new StringBuilder();
        for (final SinkRecord record : batch) {
            result.append(recordValueConverter.convert(record));
            result.append(BATCH_RECORD_SEPARATOR);
        }
        return result.toString();
    }
}
