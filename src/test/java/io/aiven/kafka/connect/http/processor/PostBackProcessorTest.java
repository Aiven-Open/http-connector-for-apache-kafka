package io.aiven.kafka.connect.http.processor;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.aiven.kafka.connect.http.config.HttpSinkConfig;
import io.aiven.kafka.connect.http.converter.PlayerContext;
import io.aiven.kafka.connect.http.converter.PostBackConverter;
import io.aiven.kafka.connect.http.converter.ProcessorUtils;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;


class PostBackProcessorTest {
    @Mock
    private HttpSinkConfig httpSinkConfig;

    protected PostBackConverter processor;
    protected final ObjectMapper objectMapper = new ObjectMapper();

    @TempDir
    Path tempDir;

    @Mock
    private JsonConverter jsonConverter;

    @Mock
    private SinkRecord sinkRecord;

    private AutoCloseable closeable;

    @BeforeEach
    void setUp() throws IOException {
      closeable = MockitoAnnotations.openMocks(this);

      // Create a temp file with some blacklisted device IDs
      File blacklistFile = tempDir.resolve("blacklisted_devices.csv").toFile();
      try (FileWriter writer = new FileWriter(blacklistFile)) {
        writer.write("abcdef123456\n");
        writer.write("123456abcdef\n");
        writer.write("00-11-22-33-44-55\n");
      }
      when(httpSinkConfig.decimalFormat()).thenReturn(org.apache.kafka.connect.json.DecimalFormat.NUMERIC);
      when(httpSinkConfig.converterType()).thenReturn("default");
      processor = new PostBackConverter( tempDir.toString(), DecimalFormat.NUMERIC); // Default

    }



  @AfterEach
  void tearDown() throws Exception {
        closeable.close();
  }

    @Test
    void loadBlacklistedDeviceIds() throws IOException {
        // Create a test file
        File blacklistFile = tempDir.resolve("test_blacklist.csv").toFile();
        try (FileWriter writer = new FileWriter(blacklistFile)) {
            writer.write("device1\n");
            writer.write("de-vi-ce-2\n");
            writer.write("DEVICE3\n");
        }

        Set<String> blacklistedIds = ProcessorUtils.loadBlacklistedDeviceIds(blacklistFile.getAbsolutePath());

        assertEquals(3, blacklistedIds.size());
        assertTrue(blacklistedIds.contains("device1"));
        assertTrue(blacklistedIds.contains("device2")); // Should be normalized without hyphens
        assertTrue(blacklistedIds.contains("device3")); // Should be normalized to lowercase
    }

    @Test
    void isValidAppStoreId() {
        // iOS app store IDs
        assertTrue(ProcessorUtils.isValidAppStoreId("id123456789"));
        assertTrue(ProcessorUtils.isValidAppStoreId("id987654321"));

        // Android app store IDs
        assertTrue(ProcessorUtils.isValidAppStoreId("com.example.app"));
        assertTrue(ProcessorUtils.isValidAppStoreId("org.test.application"));

        // Invalid app store IDs
        assertFalse(ProcessorUtils.isValidAppStoreId(""));
        assertFalse(ProcessorUtils.isValidAppStoreId("123456789")); // No "id" prefix for iOS
        assertFalse(ProcessorUtils.isValidAppStoreId(".invalid.app")); // Doesn't start with a letter
        assertFalse(ProcessorUtils.isValidAppStoreId("invalid")); // No dot
    }

    @Test
    void isValidBundleId() {
        // iOS bundle IDs
        assertTrue(ProcessorUtils.isValidBundleId("com.example.app"));
        assertTrue(ProcessorUtils.isValidBundleId("com.example-app.test"));

        // Android bundle IDs
        assertTrue(ProcessorUtils.isValidBundleId("com.example.app"));
        assertTrue(ProcessorUtils.isValidBundleId("org.test.application"));

        // Invalid bundle IDs
        assertFalse(ProcessorUtils.isValidBundleId(""));
        assertFalse(ProcessorUtils.isValidBundleId(null));
        assertFalse(ProcessorUtils.isValidBundleId("com.invalid.app$special")); // Contains invalid characters
        assertFalse(ProcessorUtils.isValidBundleId("a".repeat(156))); // Too long (more than 155 chars)
    }

    @Test
    void getPlayerContextIdAndPlayerId_withDevPartnerParameters() throws IOException {
        // Create a JSON with dev_partner_parameters
        ObjectNode fullQueryJson = objectMapper.createObjectNode();

        // Create a JSON string for dev_partner_parameters with player_id
        ObjectNode devParams = objectMapper.createObjectNode();
        devParams.put("player_id", "player123");

        fullQueryJson.put("dev_partner_parameters", objectMapper.writeValueAsString(devParams));

        PlayerContext result = ProcessorUtils.getPlayerContextIdAndPlayerId(fullQueryJson, "appStore123", "network456");

        assertEquals("appStore123", result.getContextId());
        assertEquals("player123", result.getPlayerId());
    }

//    @Test
//    void getPlayerContextIdAndPlayerId_withTrackerUserId() throws IOException {
//        ObjectNode eventRowJson = createTestEventRowJson(false, true);
//        System.out.println(eventRowJson);
//        ParamRow e = EventRow.fromJsonNode(eventRowJson);
//        ObjectMapper mapper = new ObjectMapper();
//        assertNotNull(e);
//        JsonNode fullQueryJsonNode = objectMapper.readTree(e.getfullQuery());
//        PlayerContext result2 = processor.getPlayerContextIdAndPlayerId(fullQueryJsonNode, "", "082D16568F05B720NW");
//        assertEquals("082D16568F05B720NW", result2.getContextId());
//        System.out.println(result2.getContextId());
//        assertEquals("user123", result2.getPlayerId());
//
//
//    }

    @Test
    void should_update_cart() {
        // Test bundles that should update cart
        assertTrue(ProcessorUtils.should_update_cart("com.dhgate.app"));
        assertTrue(ProcessorUtils.should_update_cart("DHGate"));
        assertTrue(ProcessorUtils.should_update_cart("someapp.DHGate.com"));

        // Test bundles that should not update cart
        assertFalse(ProcessorUtils.should_update_cart("com.example.app"));
        assertFalse(ProcessorUtils.should_update_cart(""));
        assertFalse(ProcessorUtils.should_update_cart(null));
    }

    @Test
    void processRow_withValidPlayerContext() throws Exception {
        // Create a test event row JSON
        ObjectNode eventRowJson = createTestEventRowJson(true, true);

        String requestData = objectMapper.writeValueAsString(eventRowJson);
        String result = processor.processRow(requestData);

        JsonNode resultJson = objectMapper.readTree(result);
        System.out.println("checking device:" + resultJson);
        assertEquals("id123456", resultJson.get("context_id").asText());
        assertEquals("player123", resultJson.get("player_id").asText());
        assertEquals(null , resultJson.get("device_id").asText());
        assertNotNull(resultJson.get("event_name"));
        assertNotNull(resultJson.get("bundle_id"));
        assertNotNull(resultJson.get("app_store_id"));
    }

    @Test
    void processRow_withValidDeviceId() throws Exception {
        // Create a test event row JSON with valid device ID but no player context
        ObjectNode eventRowJson = createTestEventRowJson(false, true);

        String requestData = objectMapper.writeValueAsString(eventRowJson);
        String result = processor.processRow(requestData);
        JsonNode resultJson = objectMapper.readTree(result);
        System.out.println("checking device:" + resultJson);
        assertEquals("device123", resultJson.get("device_id").asText());
        assertEquals("com.test.app", resultJson.get("bundle_id").asText());
        assertEquals("id123456", resultJson.get("app_store_id").asText());
    }

    @Test
    void processRow_withInvalidJson() {
      // Create invalid JSON strings
      String malformedJson = "{this is not valid json}";
      String incompleteJson = "{\"bundle_id\": ";
      assertThrows(Exception.class, () -> processor.processRow(malformedJson),
              "Should throw JsonParseException for malformed JSON");

      assertThrows(Exception.class, () -> processor.processRow(incompleteJson),
              "Should throw JsonParseException for incomplete JSON");

    }
  @Test
  void processRow_withNullInput() {
    // Test null input
    assertThrows(Exception.class, () -> processor.processRow(null),
            "Should throw JsonParseException for null input");
  }

  @Test
  void processRow_withValidButUnexpectedJson() throws Exception {
    String validButUnexpectedJson = "{\"unexpected_field\": \"value\"}";
    Exception exception = assertThrows(
            Exception.class,
            () -> processor.processRow(validButUnexpectedJson),
            "Error processing request - Missing appStoreId and bundleId:{\"unexpected_field\": \"value\"}\n");
  }


//  @Test
//  void convert() throws Exception {
//    // Create a spy of the processor
//    PostBackProcessor processorSpy = spy(processor);
//    ObjectNode eventRowJson = createTestEventRowJson(true, true);
//    String requestData = objectMapper.writeValueAsString(eventRowJson);
//    // Setup mock behavior
//    byte[] jsonBytes = requestData.getBytes(StandardCharsets.UTF_8);
//    // We need to inject our mock jsonConverter into the processorSpy
//    // This requires reflection since jsonConverter is a private final field
//    Field jsonConverterField = PostBackProcessor.class.getDeclaredField("jsonConverter");
//    jsonConverterField.setAccessible(true);
//    jsonConverterField.set(processorSpy, jsonConverter);
//
//
//    when(jsonConverter.fromConnectData(anyString(), any(), any())).thenReturn(jsonBytes);
//    when(sinkRecord.topic()).thenReturn("test-topic");
//    when(sinkRecord.valueSchema()).thenReturn(null);
//    when(sinkRecord.value()).thenReturn("test-value");
//
//    // Stub the processRow method on the spy
//    doReturn("{\"processed\":true}").when(processorSpy).processRow(anyString());
//
//    String result = processorSpy.convert(sinkRecord);
//
//    // Verify the result
//    assertEquals("{\"results\":\"ok\"}", result);
//
//    // Verify interactions
//    verify(jsonConverter).fromConnectData("test-topic", null, "test-value");
//    verify(processorSpy).processRow(requestData);
//  }



    // Helper method to create test event row JSON
    private ObjectNode createTestEventRowJson(boolean withPlayerContext, boolean withDeviceId) throws IOException {
        ObjectNode eventRowJson = objectMapper.createObjectNode();
        
        // Add basic fields
        eventRowJson.put("bundle_id", "com.test.app");
        eventRowJson.put("app_store_id", "id123456");
        eventRowJson.put("event_name", "test_event");
        eventRowJson.put("event_value", "test_value");
        eventRowJson.put("network_id", "network123");
        
        // Add full query with embedded JSON
        ObjectNode fullQueryJson = objectMapper.createObjectNode();
        fullQueryJson.put("city", "TestCity");
        
        if (withPlayerContext) {
            // Add player context info
            ObjectNode devParams = objectMapper.createObjectNode();
            devParams.put("player_id", "player123");
            fullQueryJson.put("dev_partner_parameters", objectMapper.writeValueAsString(devParams));
            eventRowJson.put("player_id", "player123");
        }
        else {
          ObjectNode devParams = objectMapper.createObjectNode();
          devParams.put("tracker_user_id", "user123");
          fullQueryJson.put("dev_partner_parameters", objectMapper.writeValueAsString(devParams));
        }

        eventRowJson.put("full_query", objectMapper.writeValueAsString(fullQueryJson));
        
        // Add device ID if needed
        if (withDeviceId) {
            eventRowJson.put("device_id", "device123");
        }

        System.out.println(eventRowJson.toString());
        return eventRowJson;
    }

}
