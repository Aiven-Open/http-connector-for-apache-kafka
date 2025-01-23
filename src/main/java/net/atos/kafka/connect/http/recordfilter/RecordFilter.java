package net.atos.kafka.connect.http.recordfilter;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;
import io.aiven.kafka.connect.http.converter.RecordValueConverter;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;

public abstract class RecordFilter {

    protected final RecordValueConverter recordValueConverter = new RecordValueConverter();
    protected final HttpSinkConfig config;
    protected final String filter;

    protected RecordFilter(final HttpSinkConfig config,final String filter) {
        this.filter = filter;
        this.config = config;
    }

    public abstract Collection<SinkRecord> filter(final Collection<SinkRecord> records);

    public static RecordFilter createRecordSender(final HttpSinkConfig config,String filter) {
        return new EventCloudRecordFilter(config,filter);
    }

}
