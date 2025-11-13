/*
 * Copyright 2025 Atos
 *
 */

package net.atos.kafka.connect.http.recordfilter;

import java.util.Collection;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;
import io.aiven.kafka.connect.http.converter.RecordValueConverter;



public abstract class RecordFilter {

    protected final RecordValueConverter recordValueConverter;
    protected final HttpSinkConfig config;
    protected final String filter;

    protected RecordFilter(final HttpSinkConfig config, final String filter) {
        this.filter = filter;
        this.config = config;
        this.recordValueConverter = RecordValueConverter.create(config);
    }

    public abstract Collection<SinkRecord> filter(final Collection<SinkRecord> records);

    public static RecordFilter createRecordFilter(final HttpSinkConfig config, final String filter) {
        return new EventCloudRecordFilter(config, filter);
    }

}
