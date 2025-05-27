package io.aiven.kafka.connect.http.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static io.aiven.kafka.connect.http.converter.ProcessorUtils.*;

public class EventRow {
    private static final Logger logger = LoggerFactory.getLogger(EventRow.class);
    private final String deviceId;
    private final String appStoreId;
    private final String eventId;
    private final String eventName;
    private final String eventValue;
    private final String advertisingId;
    private final String bundleId;
    private final String usdNumValue;
    private final String eventTime;
    private final String loggingTime;
    private final String attribution;
    private final String userIp;
    private final String deviceOsVersion;
    private final String deviceModel;
    private final String deviceLanguage;
    private final String carrier;
    private final String networkId;
    private final String clAdvId;
    private final String idfv;
    private final String city;
    private final String additionalProductData;
    private final String fullQuery;
    private final String playerId;
    private final String contextId;


    public EventRow(String deviceId, String appStoreId, String eventId, String eventName, String eventValue,
                    String advertisingId, String bundleId, String usdNumValue, String eventTime, String loggingTime,
                    String attribution, String userIp, String deviceOsVersion, String deviceModel,
                    String deviceLanguage, String carrier, String networkId, String clAdvId, String idfv,
                    String city, String additionalProductData, String fullQuery, String playerId, String contextId) {
        this.deviceId = deviceId;
        this.appStoreId = appStoreId;
        this.eventId = eventId;
        this.eventName = eventName;
        this.eventValue = eventValue;
        this.advertisingId = advertisingId;
        this.bundleId = bundleId;
        this.usdNumValue = usdNumValue;
        this.eventTime = eventTime;
        this.loggingTime = loggingTime;
        this.attribution = attribution;
        this.userIp = userIp;
        this.deviceOsVersion = deviceOsVersion;
        this.deviceModel = deviceModel;
        this.deviceLanguage = deviceLanguage;
        this.carrier = carrier;
        this.networkId = networkId;
        this.clAdvId = clAdvId;
        this.idfv = idfv;
        this.city = city;
        this.additionalProductData = additionalProductData;
        this.fullQuery = fullQuery;
        this.playerId = playerId;
        this.contextId = contextId;
    }

    public String getfullQuery() {
        return fullQuery;
    }
    public String getPlayerId() {
        return playerId;
    }
    public String getContextId() {
        return contextId;
    }

    public String getAppStoreId() {
        return appStoreId;
    }

    public String getEventId() {
        return eventId;
    }

    public String getEventName() {
        return eventName;
    }

    public String getEventValue() {
        return eventValue;
    }

    public String getAdvertisingId() {
        return advertisingId;
    }

    public String getBundleId() {
        return bundleId;
    }

    public String getUsdNumValue() {
        return usdNumValue;
    }

    public String getEventTime() {
        return eventTime;
    }

    public String getLoggingTime() {
        return loggingTime;
    }

    public String getAttribution() {
        return attribution;
    }

    public String getUserIp() {
        return userIp;
    }

    public String getDeviceOsVersion() {
        return deviceOsVersion;
    }

    public String getDeviceModel() {
        return deviceModel;
    }

    public String getDeviceLanguage() {
        return deviceLanguage;
    }

    public String getCarrier() {
        return carrier;
    }

    public String getNetworkId() {
        return networkId;
    }

    public String getClAdvId() {
        return clAdvId;
    }

    public String getIdfv() {
        return idfv;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public String getCity() {
        return city;
    }

    public String getAdditionalProductData() {
        return additionalProductData;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventRow eventRow = (EventRow) o;
        return
                Objects.equals(deviceId, eventRow.deviceId) &&
                Objects.equals(appStoreId, eventRow.appStoreId) &&
                Objects.equals(eventId, eventRow.eventId) &&
                Objects.equals(eventName, eventRow.eventName) &&
                Objects.equals(eventValue, eventRow.eventValue) &&
                Objects.equals(advertisingId, eventRow.advertisingId) &&
                Objects.equals(bundleId, eventRow.bundleId) &&
                Objects.equals(usdNumValue, eventRow.usdNumValue) &&
                Objects.equals(eventTime, eventRow.eventTime) &&
                Objects.equals(loggingTime, eventRow.loggingTime) &&
                Objects.equals(attribution, eventRow.attribution) &&
                Objects.equals(userIp, eventRow.userIp) &&
                Objects.equals(deviceOsVersion, eventRow.deviceOsVersion) &&
                Objects.equals(deviceModel, eventRow.deviceModel) &&
                Objects.equals(deviceLanguage, eventRow.deviceLanguage) &&
                Objects.equals(carrier, eventRow.carrier) &&
                Objects.equals(networkId, eventRow.networkId) &&
                Objects.equals(clAdvId, eventRow.clAdvId) &&
                Objects.equals(idfv, eventRow.idfv) &&
                Objects.equals(city, eventRow.city) &&
                Objects.equals(additionalProductData, eventRow.additionalProductData) &&
                Objects.equals(fullQuery, eventRow.fullQuery) &&
                Objects.equals(contextId, eventRow.contextId) &&
                Objects.equals(playerId, eventRow.playerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deviceId, appStoreId, eventId, eventName, eventValue, advertisingId, bundleId, usdNumValue,
                eventTime, loggingTime, attribution, userIp, deviceOsVersion, deviceModel, deviceLanguage, carrier,
                networkId, clAdvId, idfv, city, additionalProductData, fullQuery, contextId, playerId);
    }

    @Override
    public String toString() {
        List<Object> result = new ArrayList<>();
        result.add(deviceId);
        result.add(appStoreId);
        result.add(bundleId);
        result.add(eventName);
        result.add(eventValue);
        result.add(usdNumValue);
        result.add(eventId);
        result.add(eventTime);
        result.add(loggingTime);
        result.add(eventTime);
        result.add(attribution);
        result.add(deviceOsVersion);
        result.add(deviceModel);
        result.add(deviceLanguage);
        result.add(carrier);
        result.add(city);
        result.add(userIp);
        result.add(additionalProductData);
        result.add(idfv);
       /* result.add(advertisingId);
        result.add(networkId);
        result.add(clAdvId);
        result.add(contextId);
        result.add(playerId);
         params = (app_store_id, bundle_id, event_name, usd_num_value or event_value, event_id,
                          event_time or logging_time, attribution, os_version, device_model, device_language, carrier,
                          city, user_ip, additional_product_data, idfv)
         */
        return Arrays.toString(result.toArray());

    }

    public static JsonNode parseEventRowJson(String requestData) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readTree(requestData);
        } catch (IOException e) {
            logger.error("Failed to parse JSON for event row: " + e.getMessage(), e);
            return null;
        }
    }
    private boolean isNullOrEmpty(String str) {
        return str == null || str.isEmpty();
    }

    private static String getValueOrDefault(String value, String defaultValue) {
        return (value == null || value.isEmpty()) ? defaultValue : value;
    }


    public static IParamRow fromJsonNode(JsonNode eventRowJson) throws Exception {
            ObjectMapper objectMapper = new ObjectMapper();

            String appStoreId = Optional.ofNullable(eventRowJson.get("app_store_id"))
                    .map(JsonNode::asText).orElse("");
            String eventId = Optional.ofNullable(eventRowJson.get("event_id"))
                    .map(JsonNode::asText).orElse("");
            String eventName = Optional.ofNullable(eventRowJson.get("event_name"))
                    .map(JsonNode::asText).orElse("");
            String eventValue = Optional.ofNullable(eventRowJson.get("event_value"))
                    .map(JsonNode::asText).orElse("");
            String bundleId = Optional.ofNullable(eventRowJson.get("bundle_id"))
                    .map(JsonNode::asText).orElse("");
            String usdNumValue = Optional.ofNullable(eventRowJson.get("usd_num_value"))
                    .map(JsonNode::asText).orElse("");
            String eventTime = Optional.ofNullable(eventRowJson.get("event_time"))
                    .map(JsonNode::asText).orElse("");
            String loggingTime = Optional.ofNullable(eventRowJson.get("logging_time"))
                    .map(JsonNode::asText).orElse("");
            String attribution = Optional.ofNullable(eventRowJson.get("attribution"))
                    .map(JsonNode::asText).orElse("");
            String userIp = Optional.ofNullable(eventRowJson.get("user_ip"))
                    .map(JsonNode::asText).orElse("");
            String deviceOsVersion = Optional.ofNullable(eventRowJson.get("device_os_version"))
                    .map(JsonNode::asText).orElse("");
            String deviceModel = Optional.ofNullable(eventRowJson.get("device_model"))
                    .map(JsonNode::asText).orElse("");
            String deviceLanguage = Optional.ofNullable(eventRowJson.get("device_language"))
                    .map(JsonNode::asText).orElse("");
            String carrier = Optional.ofNullable(eventRowJson.get("carrier"))
                    .map(JsonNode::asText).orElse("");
            String networkId = Optional.ofNullable(eventRowJson.get("network_id"))
                    .map(JsonNode::asText).orElse("");
//            String clAdvId = Optional.ofNullable(eventRowJson.get("cl_adv_id"))
//                    .map(JsonNode::asText).orElse("");
            String idfv = Optional.ofNullable(eventRowJson.get("idfv"))
                    .map(JsonNode::asText).orElse("");
//            String contextId = Optional.ofNullable(eventRowJson.get("context_id"))
//                    .map(JsonNode::asText).orElse("");
//            String playerId = Optional.ofNullable(eventRowJson.get("player_id"))
//                    .map(JsonNode::asText).orElse("");


            // Process full_query if present
            String fullQuery = Optional.ofNullable(eventRowJson.get("full_query"))
                    .map(JsonNode::asText).orElse("");


            // Default values for fields normally extracted from fullQuery
            String city = "";
            String additionalProductData = "";
            String deviceId = Optional.ofNullable(eventRowJson.get("device_id"))
                    .map(JsonNode::asText).orElse("");;
//            String advertisingId = Optional.ofNullable(eventRowJson.get("advertising_id"))
//                    .map(JsonNode::asText).orElse("");;

            // Try to parse fullQuery if it's not empty
            if (!fullQuery.isEmpty()) {
                try {
                    JsonNode fullQueryJson = objectMapper.readTree(fullQuery);

                    // Extract values from fullQuery
                    city = Optional.ofNullable(fullQueryJson.get("city"))
                            .map(JsonNode::asText).orElse("");

                    additionalProductData = Optional.ofNullable(fullQueryJson.get("event_value"))
                            .map(JsonNode::asText).orElse("");
                    deviceId = Optional.ofNullable(fullQueryJson.get("device_id"))
                            .map(JsonNode::asText).orElse("");
//                    advertisingId = Optional.ofNullable(fullQueryJson.get("advertising_id"))
//                            .map(JsonNode::asText).orElse("");

                } catch (Exception e) {
                    logger.error("Failed to parse fullQuery into JSON: " + e.getMessage(), e);
                }
            }
            eventValue = getValueOrDefault(eventValue, usdNumValue);
            eventTime = getValueOrDefault(eventTime, loggingTime);
            //Fields Fetched or Computed: bundleId, appStoreId, city, eventName, eventValue,
            // additionalProductData, deviceId, playerContext if it contains player_id

        bundleId = isValidBundleId(bundleId) ? bundleId : "";
        if (!isValidAppStoreId(appStoreId))
            appStoreId = bundleId;

        if (appStoreId.isEmpty() && bundleId.isEmpty()) {
            logger.error("Error processing request - Missing appStoreId and bundleId:{}", eventRowJson.toString());
            throw new Exception("Error processing request - Missing appStoreId and bundleId:" + eventRowJson);
        }

        if (!should_update_cart(bundleId))
            additionalProductData = "";

        if (eventValue.contains("\"af_"))
            eventValue = "N/A";

        if (!isUpdatableDeviceId(deviceId)) {
            logger.error("Error processing request - Invalid deviceId:{}", deviceId);
            return null;
        }

        //choose app store Id or NetworkID for player context
        PlayerContext playerContext = ProcessorUtils.getPlayerContextIdAndPlayerId(objectMapper.readTree(fullQuery), appStoreId, networkId);
        // check if network id is in the player network ids and is valid
        if (playerContext != null && PLAYER_NETWORK_IDS.contains(networkId)) {
            if (playerContext.getPlayerId().contains("\ufffd")) {
                logger.info("BAD LINE:{} ", Arrays.asList(deviceId, playerContext.getContextId(), playerContext.getPlayerId()));
                throw new Exception("BAD LINE: {}" + Arrays.asList(deviceId, playerContext.getContextId(), playerContext.getPlayerId()));
            }

            PlayerParamRow player = new PlayerParamRow(
                    playerContext.getPlayerId(),    //player_id
                    playerContext.getContextId(), //context_id
                    deviceId,             // device_id
                    appStoreId,          // app_store_id
                    bundleId,            // bundle_id
                    eventName,           // event_name
                    eventValue,          // event_value
                    eventId,             // event_id
                    eventTime,           // event_time
                    attribution,         // attribution
                    deviceOsVersion,     // os_version
                    deviceModel,         // device_model
                    deviceLanguage,      // device_language
                    carrier,             // carrier
                    city,                // city
                    userIp,              // user_ip
                    additionalProductData, // additional_product_data
                    idfv                  // idfv
            );
            logger.info("Parsed Player Row Successfully:{} ", player.toString());
            return player;

        }


        //# TODO: send to bad data topic - dqe
        // result.put("results", "error-missing-player-id-and-device-id");
        // #TODO: check if these fields are required - Add all  fields to the result
        //      result.put("full_query", eventRow.getfullQuery());
        //      result.put("advertising_id", eventRow.getAdvertisingId());
        //      result.put("event_time", eventRow.getEventTime()); or/both
        //      result.put("logging_time", eventRow.getLoggingTime());
        //      result.put("network_id", eventRow.getNetworkId());
        //      result.put("cl_adv_id", eventRow.getClAdvId());
        DeviceParamRow device = new DeviceParamRow(
                deviceId,             // device_id
                appStoreId,          // app_store_id
                bundleId,            // bundle_id
                eventName,           // event_name
                eventValue,          // event_value
                eventId,             // event_id
                eventTime,           // event_time
                attribution,         // attribution
                deviceOsVersion,     // os_version
                deviceModel,         // device_model
                deviceLanguage,      // device_language
                carrier,             // carrier
                city,                // city
                userIp,              // user_ip
                additionalProductData, // additional_product_data
                idfv                 // idfv
        );
        logger.info("Parsed Device Row Successfully: " + device.toString());
        return device;
    }
}