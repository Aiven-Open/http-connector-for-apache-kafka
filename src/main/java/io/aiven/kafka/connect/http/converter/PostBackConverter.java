package io.aiven.kafka.connect.http.converter;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.aiven.kafka.connect.http.converter.ProcessorUtils.*;


/**
 * The PostBackProcessor class is responsible for processing and validating
 * event data related to app store or bundle identifiers, player contexts,
 * and device IDs. This class implements the RecordValueConverter.Converter interface
 * and provides functionalities for extracting information, handling event row
 * processing, and updating relevant data.
 *
 * This processor also handles the initialization of blacklisted device IDs
 * and validates attributes based on predefined patterns or conditions.
 */
public class PostBackConverter extends JsonConverter implements RecordValueConverter.Converter{

  private static final Logger logger = LoggerFactory.getLogger(PostBackConverter.class);
  private final JsonConverter jsonConverter;

  private String workingDirPrefix;

  public PostBackConverter() {
    super();
    this.workingDirPrefix = workingDirPrefix != null ? workingDirPrefix : DEFAULT_WORKING_DIR_PREFIX;
    Map<String, Object> configs = new HashMap<>();
    configs.put("schemas.enable", false);
    configs.put("converter.type", "value");
    configs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
    super.configure(configs, false);
    this.jsonConverter = new JsonConverter();
    jsonConverter.configure(configs, false);

  }

  /**
   * Constructor with configuration parameters
   */
  public PostBackConverter(final String workingDirPrefix, final DecimalFormat decimalFormat) {
    this.workingDirPrefix = workingDirPrefix != null ? workingDirPrefix : DEFAULT_WORKING_DIR_PREFIX;
    Map<String, Object> configs = new HashMap<>();
    configs.put("schemas.enable", false);
    configs.put("converter.type", "value");
    configs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, decimalFormat.name());
    super.configure(configs, false);
    this.jsonConverter = new JsonConverter();
    jsonConverter.configure(configs, false);
  }


  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    super.configure(configs, isKey);
    if (configs.containsKey("working.dir.prefix")) {
      this.workingDirPrefix = (String) configs.get("working.dir.prefix");
    }

  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    if (topic == null || !shouldProcessTopic(topic)) {
      logger.info("Skipping topic: {}", topic);
      return super.fromConnectData(topic, schema, value);
    }

    try {
      logger.info("Processing topic: {}", topic);
      byte[] jsonBytes = super.fromConnectData(topic, schema, value);
      String jsonString = new String(jsonBytes, StandardCharsets.UTF_8);
      String processedJson = processRow(jsonString);
      logger.info("Processed json: {}", processedJson);

      return processedJson.getBytes(StandardCharsets.UTF_8);
    } catch (Exception e) {
      logger.error("Error in PostBackProcessor conversion: ", e);
      return super.fromConnectData(topic, schema, value);
    }
  }

  @Override
  public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
    if (topic == null || !shouldProcessTopic(topic)) {
      logger.info("Skipping topic: {}", topic);
      return super.fromConnectData(topic, headers, schema, value);
    }
    try {
      logger.info("Processing topic: {}", topic);
      byte[] jsonBytes = super.fromConnectData(topic, headers, schema, value);
      String jsonString = new String(jsonBytes, StandardCharsets.UTF_8);
      String processedJson = processRow(jsonString);
      logger.info("Processed json: {}", processedJson);

      return processedJson.getBytes(StandardCharsets.UTF_8);
    } catch (Exception e) {
      logger.error("Error in PostBackProcessor conversion with headers: ", e);
      return super.fromConnectData(topic, headers, schema, value);
    }
  }

  public String processRow(String requestData) throws Exception {
    // Create a full Param Row object with all fields based on device or player
    try{
        JsonNode jsonNode = EventRow.parseEventRowJson(requestData);
        IParamRow params= EventRow.fromJsonNode(jsonNode);
        ObjectMapper objectMapper = new ObjectMapper();
        logger.info("Parsed EventRow Successfully: {}", params.toString());
        return objectMapper.writeValueAsString(params);
      }
    catch (JsonParseException e) {
      logger.error("Failed to parse request data as JSON: {}", requestData);
      throw new JsonParseException("Failed to parse request data as JSON: " + requestData);
    }
    catch (Exception e) {
        logger.error("Error processing request: {}", e.getMessage(), e);
        throw new Exception("Error processing request: " + e.getMessage(), e);
      }
  }

  @Override
  public String convert(final SinkRecord record) {
    try {
        return processRow(new String(
                jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value()),
                StandardCharsets.UTF_8));

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}


