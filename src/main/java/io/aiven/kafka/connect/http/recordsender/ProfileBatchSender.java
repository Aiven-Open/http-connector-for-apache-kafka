package io.aiven.kafka.connect.http.recordsender;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.aiven.kafka.connect.http.converter.RecordValueConverter;
import io.aiven.kafka.connect.http.sender.HttpSender;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ProfileBatchSender extends RecordSender {
  private static final Logger log = LoggerFactory.getLogger(ProfileBatchSender.class);
  private final int batchMaxSize;
  private final String batchPrefix;
  private final String batchSuffix;
  private final String batchSeparator;

  protected ProfileBatchSender(final HttpSender httpSender,
                              final RecordValueConverter recordValueConverter,
                              final int batchMaxSize,
                              final String batchPrefix,
                              final String batchSuffix,
                              final String batchSeparator) {
    super(httpSender, recordValueConverter);
    this.batchMaxSize = batchMaxSize;
    this.batchPrefix = batchPrefix;
    this.batchSuffix = batchSuffix;
    this.batchSeparator = batchSeparator;
  }

  @Override
  public void send(final Collection<SinkRecord> records) throws JsonProcessingException {
    final List<SinkRecord> batch = new ArrayList<>(batchMaxSize);
    for (final var record : records) {
      batch.add(record);
      if (batch.size() >= batchMaxSize) {
        log.debug("batch:{}", batch);
        final String body = createRequestBody(batch);
        batch.clear();
        log.debug("body:{}", body);
        httpSender.send(body);
      }
    }
    if (!batch.isEmpty()) {
      log.debug("batch:{}", batch);
      final String body = createRequestBody(batch);
      log.debug("body:{}", body);
      httpSender.send(body);
    }
  }

  @Override
  public void send(final SinkRecord record) {
    throw new ConnectException("Don't call this method for batch sending");
  }

  private ArrayNode createDeviceOperation(SinkRecord record, ObjectMapper objectMapper) throws JsonProcessingException {
    String  convertedRecord = recordValueConverter.convert(record);
    ArrayNode operation = objectMapper.createArrayNode();
    ArrayNode nested = objectMapper.createArrayNode();
    JsonNode event = objectMapper.readTree(convertedRecord);
    Iterator<JsonNode> it = event.iterator();
    if (it.hasNext()) {
      JsonNode firstNode = it.next();
      log.debug("device_id:{}", firstNode);
      operation.add(firstNode);
    }
    operation.add("update_app_event_stats");
    while (it.hasNext()) {
      JsonNode node = it.next();
      nested.add(node);
    }
    operation.add(nested);
    return operation;
  }

  private String createRequestBody(final Collection<SinkRecord> batch) throws JsonProcessingException {
    StringBuilder result = new StringBuilder();
    if (!batchPrefix.isEmpty()) {
      result.append(batchPrefix);
    }

    ObjectMapper objectMapper = new ObjectMapper();
    ArrayNode operations = objectMapper.createArrayNode();
    final Iterator<SinkRecord> it = batch.iterator();
    if (it.hasNext()) {
      operations.add(createDeviceOperation(it.next(), objectMapper));
      while (it.hasNext()) {
        result.append(batchSeparator);
        operations.add(createDeviceOperation(it.next(), objectMapper));
      }
    }

    ObjectNode rootNode = objectMapper.createObjectNode();
    rootNode.put("operations", operations);
    rootNode.put("max_tries", 2);
    result.append(objectMapper.writeValueAsString(objectMapper.writeValueAsString(rootNode)));

    if (!batchSuffix.isEmpty()) {
      result.append(batchSuffix);
    }
     log.debug("result:{}", result.toString());
     return result.toString();
  }
}

