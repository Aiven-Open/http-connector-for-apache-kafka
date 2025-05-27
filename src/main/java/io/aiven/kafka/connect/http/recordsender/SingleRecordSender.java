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

import io.aiven.kafka.connect.http.converter.RecordValueConverter;
import io.aiven.kafka.connect.http.sender.HttpSender;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public final class SingleRecordSender extends RecordSender {
    private static final Logger log = LoggerFactory.getLogger(SingleRecordSender.class);

    protected SingleRecordSender(final HttpSender httpSender, final RecordValueConverter recordValueConverter) {
        super(httpSender, recordValueConverter);
    }

    @Override
    public void send(final Collection<SinkRecord> records) {
        for (final SinkRecord record : records) {
            final String body = recordValueConverter.convert(record);
            log.debug("body:" + recordValueConverter.convert(record));
            httpSender.send(body);
        }
    }

    @Override
    public void send(final SinkRecord record) {
        final String body = recordValueConverter.convert(record);
        log.debug("body:" + recordValueConverter.convert(record));
        httpSender.send(body);
    }
}
