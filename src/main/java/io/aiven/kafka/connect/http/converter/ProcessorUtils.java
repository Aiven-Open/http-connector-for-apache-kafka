package io.aiven.kafka.connect.http.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

public class ProcessorUtils {
  //private final Set<String> blacklistedDeviceIds;
  private static final Logger logger = LoggerFactory.getLogger(ProcessorUtils.class);
  public static final String DEFAULT_WORKING_DIR_PREFIX = "/var/lib/luigi/app_event_upload_status";
  public static final Pattern IOS_STORE_ID_RE = Pattern.compile("^id\\d+$");
  public static final Pattern IOS_BUNDLE_ID_RE = Pattern.compile("^[a-z0-9A-Z.\\-]{1,155}$");
  public static final Pattern ANDROID_STORE_ID_RE = Pattern.compile("^[a-zA-Z]\\w*(\\.[a-zA-Z]\\w*)+$");
  public static final Set<String> INVALID_DEVICE_IDS = new HashSet<>(Arrays.asList(
          "00000000-0000-0000-0000-000000000000", "0000-0000", "nil", "0"
  ));
  public static final Set<String> ACCOUNT_LEVEL_TRACKER_USER_ID = new HashSet<>(Arrays.asList("082D16568F05B720NW"));

  public static final Set<String> PLAYER_NETWORK_IDS = new HashSet<>(Arrays.asList(
          "082D16568F05B720NW",   // Niantic
          "413662E5C3FA8E3ENW",  // SciPlay-US
          "F70DF9E77E4E8375NW"   // SciPlay-IL
  ));
  // #TODO: update device ids from kafka
  public static void downloadDeviceIdsBlacklist() {
    File localBlacklistFile = new File("localBlacklistPath");
    if (!localBlacklistFile.exists()) {
      //Read blacklisteddeviceIds from kafka or elsewhere
      //Set<String> blacklistedDeviceIds = factRdb.smembers(FACT_BLACKLISTED_DEVICE_IDS);
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(localBlacklistFile))) {
        //writer.write(String.join("\n", blacklistedDeviceIds));
      } catch (IOException e) {
        logger.error("Failed to write device IDs blacklist to file: " + e.getMessage(), e);
      }
    }

  }

  /**
   * Determine if this topic should be processed by our custom logic
   */
  public static  boolean shouldProcessTopic(String topic) {
    // Add logic to determine which topics to process
    return topic.startsWith("postbacks") ||
            topic.contains("events") ||
            topic.contains("tracking");
  }


  //    // Load the blacklisted device IDs from a file during initialization
  ////    final String workingDir = Paths.get(Optional.ofNullable(working_dir_prefix).
  ////            orElse(DEFAULT_WORKING_DIR_PREFIX)).toAbsolutePath().toString();
  ////    final String localBlacklistPath = Paths.get(workingDir, "blacklisted_devices.csv").toString();
  ////    this.blacklistedDeviceIds = loadBlacklistedDeviceIds(localBlacklistPath);
//  }



  public static Set<String> loadBlacklistedDeviceIds(String filePath) throws IOException {
    Set<String> blacklistedIds = new HashSet<>();
    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
      String line;
      while ((line = reader.readLine()) != null) {
        blacklistedIds.add(normalizeDeviceId(line.trim()));
      }
    }
    return blacklistedIds;
  }

  public static boolean isValidAppStoreId(String appStoreId) {
    return isNotNullAndNotEmpty(appStoreId) && (IOS_STORE_ID_RE.matcher(appStoreId).matches() ||
            ANDROID_STORE_ID_RE.matcher(appStoreId).matches());
  }

  public static boolean isValidBundleId(String bundleId) {
    return isNotNullAndNotEmpty(bundleId) && (IOS_BUNDLE_ID_RE.matcher(bundleId).matches() ||
            ANDROID_STORE_ID_RE.matcher(bundleId).matches());
  }

  public static String normalizeDeviceId(String deviceId) {
    return deviceId == null ? null : deviceId.trim().toLowerCase().replaceAll("-", "");
  }

  public static boolean isUpdatableDeviceId(String deviceId) {
    // The device id is updatable if it's not null, not a UUID, and not a blacklisted ID'
    return (deviceId != null && !INVALID_DEVICE_IDS.contains(deviceId) &&
            !deviceId.startsWith("rnd_"));
//            &&
//            !blacklistedDeviceIds.contains(normalizeDeviceId(deviceId));
  }

  public static PlayerContext getPlayerContextIdAndPlayerId(JsonNode fullQuery, String appStoreId, String networkId) {
    String playerId = "";
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      String devPartnerParams = Optional.ofNullable(fullQuery.get("dev_partner_parameters"))
              .map(JsonNode::asText).orElse("");
      logger.debug("dev_partner_parameters: {}", devPartnerParams);
      JsonNode devPartnerParamsNode = objectMapper.readTree(devPartnerParams);
      if (devPartnerParamsNode.has("player_id")) {
        playerId = Optional.ofNullable(devPartnerParamsNode.get("player_id")).map(JsonNode::asText).orElse("");
      }
      if (devPartnerParamsNode.has("tracker_user_id"))
        playerId = Optional.ofNullable(devPartnerParamsNode.get("tracker_user_id")).map(JsonNode::asText).orElse("");

    } catch (Exception e) {
      logger.error("Failed to parse dev_partner_parameters: " + e.getMessage(), e);
    }
    return new PlayerContext(
            ACCOUNT_LEVEL_TRACKER_USER_ID.contains(networkId) ? networkId : appStoreId,
            playerId);
  }



  // For certain publishers, dhgate for example, we want to parse their event value and extract cart
  // and content view information
  public static boolean should_update_cart(String bundle) {
    //# more sophisticated way of checking ?
    //# maybe check with full bunlde comparison ?
    return isNotNullAndNotEmpty(bundle) && (bundle.toLowerCase().contains("dhgate") ||
            bundle.toLowerCase().contains("dhgate.com"));
  }

  public static boolean isNotNullAndNotEmpty(String value) {
    return value != null && !value.isEmpty();
  }
}
