package net.atos.kafka.connect.http.recordfilter;

import com.jayway.jsonpath.JsonPath;
import io.aiven.kafka.connect.http.config.HttpSinkConfig;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class EventCloudRecordFilter extends RecordFilter {
    protected EventCloudRecordFilter(final HttpSinkConfig config,String filter) {
        super(config,filter);
    }

    private boolean doFilter(SinkRecord sinkRecord){

        String json = recordValueConverter.convert(sinkRecord);
        String newFilterJson = "{ \"items\" : ["+json+"]}";
        return !JsonPath.parse(newFilterJson).read("$.items[?("+filter+")]", List.class).isEmpty();
    }
    public Collection<SinkRecord> filter(final Collection<SinkRecord> records){
        return records.stream().filter( r -> r.value()!=null && doFilter(r)).collect(Collectors.toList());
    }

}
