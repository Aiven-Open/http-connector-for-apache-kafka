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

import java.util.Collection;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.http.sender.HttpSender;

final class SingleRecordSender extends RecordSender {

    protected SingleRecordSender(final HttpSender httpSender) {
        super(httpSender);
    }

    @Override
    public void send(final Collection<SinkRecord> records) {
        for (final SinkRecord record : records) {
            final String body = recordValueConverter.convert(record);
            httpSender.send(body);
        }
    }

    @Override
    public void send(final SinkRecord record) {
        final String body = recordValueConverter.convert(record);
        httpSender.send(body);
    }
}
