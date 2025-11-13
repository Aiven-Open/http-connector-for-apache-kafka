/*
 * Copyright 2025 Atos
 *
 */

package net.atos.kafka.connect.http.recordfilter;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import com.jayway.jsonpath.JsonPath;


public class EventCloudRecordFilter extends RecordFilter {

    protected EventCloudRecordFilter(final HttpSinkConfig config, final String filter) {
        super(config, filter);
    }

    private boolean doFilter(final SinkRecord sinkRecord) {

        final String json = recordValueConverter.convert(sinkRecord);
        final String newFilterJson = "{ \"items\" : [" + json + "] }";
        return !JsonPath.parse(newFilterJson).read("$.items[?(" + filter + ")]", List.class).isEmpty();
    }

    public Collection<SinkRecord> filter(final Collection<SinkRecord> records) {
        return records.stream()
                .filter(r -> r.value() != null && doFilter(r))
                .collect(Collectors.toList());
    }
}
