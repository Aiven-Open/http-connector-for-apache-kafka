package io.aiven.kafka.connect.http.converter;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class DeviceParamRow implements IParamRow{
  private String device_id;
  private String app_store_id;
  private String bundle_id;
  private String event_name;
  private String usd_num_value;
  private String event_value;
  private String event_id;
  private String event_time;
  private String logging_time;
  private String attribution;
  private String os_version;
  private String device_model;
  private String device_language;
  private String carrier;
  private String city;
  private String user_ip;
  private String idfv;
  private String additional_product_data;

  public DeviceParamRow(String appStoreId, String bundleId, String eventName, String eventValue, String eventId, String eventTime, String attribution, String deviceOsVersion, String deviceModel, String deviceLanguage, String carrier, String city, String userIp, String additionalProductData, String idfv) {
  }

  public DeviceParamRow(String device_id, String app_store_id, String bundle_id, String event_name,
                        String event_value, String event_id,
                        String event_time, String attribution,
                        String os_version, String device_model, String device_language,
                        String carrier, String city, String user_ip, String additional_product_data, String idfv
                 ) {
    this.device_id = device_id;
    this.app_store_id = app_store_id;
    this.bundle_id = bundle_id;
    this.event_name = event_name;
    this.event_value = event_value;
    this.event_id = event_id;
    this.event_time = event_time;
    this.attribution = attribution;
    this.os_version = os_version;
    this.device_model = device_model;
    this.device_language = device_language;
    this.carrier = carrier;
    this.city = city;
    this.user_ip = user_ip;
    this.additional_product_data = additional_product_data;
    this.idfv = idfv;

  }


  public String getAdditional_product_data () {
    return additional_product_data;
  }

  public void setAdditional_product_data (String additional_product_data){
    this.additional_product_data = additional_product_data;
  }

  public String getDevice_id() {
    return device_id;
  }

  public void setDevice_id(String device_id) {
    this.device_id = device_id;
  }

  public String getApp_store_id() {
    return app_store_id;
  }

  public void setApp_store_id(String app_store_id) {
    this.app_store_id = app_store_id;
  }

  public String getBundle_id() {
    return bundle_id;
  }

  public void setBundle_id(String bundle_id) {
    this.bundle_id = bundle_id;
  }

  public String getEvent_name() {
    return event_name;
  }

  public void setEvent_name(String event_name) {
    this.event_name = event_name;
  }

  public String getUsd_num_value() {
    return usd_num_value;
  }

  public void setUsd_num_value(String usd_num_value) {
    this.usd_num_value = usd_num_value;
  }

  public String getEvent_value() {
    return event_value;
  }

  public void setEvent_value(String event_value) {
    this.event_value = event_value;
  }

  public String getEvent_id() {
    return event_id;
  }

  public void setEvent_id(String event_id) {
    this.event_id = event_id;
  }

  public String getEvent_time() {
    return event_time;
  }

  public void setEvent_time(String event_time) {
    this.event_time = event_time;
  }

  public String getLogging_time() {
    return logging_time;
  }

  public void setLogging_time(String logging_time) {
    this.logging_time = logging_time;
  }

  public String getAttribution() {
    return attribution;
  }

  public void setAttribution(String attribution) {
    this.attribution = attribution;
  }

  public String getOs_version() {
    return os_version;
  }

  public void setOs_version(String os_version) {
    this.os_version = os_version;
  }

  public String getDevice_model() {
    return device_model;
  }

  public void setDevice_model(String device_model) {
    this.device_model = device_model;
  }

  public String getDevice_language() {
    return device_language;
  }

  public void setDevice_language(String device_language) {
    this.device_language = device_language;
  }

  public String getCarrier() {
    return carrier;
  }

  public void setCarrier(String carrier) {
    this.carrier = carrier;
  }

  public String getCity() {
    return city;
  }

  public void setCity(String city) {
    this.city = city;
  }

  public String getUser_ip() {
    return user_ip;
  }

  public void setUser_ip(String user_ip) {
    this.user_ip = user_ip;
  }

  public String getIdfv() {
    return idfv;
  }

  public void setIdfv(String idfv) {
    this.idfv = idfv;
  }

  private boolean isNullOrEmpty(String str) {
    return str == null || str.isEmpty();
  }

  @Override
  public String toString() {
    List<Object> result = new ArrayList<>();
    result.add(device_id);
    result.add(app_store_id);
    result.add(bundle_id);
    result.add(event_name);
    result.add(isNullOrEmpty(event_value) ? usd_num_value : event_value);
    result.add(event_id);
    result.add(isNullOrEmpty(event_time) ? logging_time : event_time);
    result.add(attribution);
    result.add(os_version);
    result.add(device_model);
    result.add(device_language);
    result.add(carrier);
    result.add(city);
    result.add(user_ip);
    result.add(additional_product_data);
    result.add(idfv);
    return Arrays.toString(result.toArray());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DeviceParamRow paramRow = (DeviceParamRow) o;
    return
            Objects.equals(device_id, paramRow.device_id) &&
            Objects.equals(app_store_id, paramRow.app_store_id) &&
            Objects.equals(bundle_id, paramRow.bundle_id) &&
            Objects.equals(event_name, paramRow.event_name) &&
            Objects.equals(usd_num_value, paramRow.usd_num_value) &&
            Objects.equals(event_value, paramRow.event_value) &&
            Objects.equals(event_id, paramRow.event_id) &&
            Objects.equals(event_time, paramRow.event_time) &&
            Objects.equals(logging_time, paramRow.logging_time) &&
            Objects.equals(attribution, paramRow.attribution) &&
            Objects.equals(os_version, paramRow.os_version) &&
            Objects.equals(device_model, paramRow.device_model) &&
            Objects.equals(device_language, paramRow.device_language) &&
            Objects.equals(carrier, paramRow.carrier) &&
            Objects.equals(city, paramRow.city) &&
            Objects.equals(user_ip, paramRow.user_ip) &&
            Objects.equals(additional_product_data, paramRow.additional_product_data) &&
            Objects.equals(idfv, paramRow.idfv);
  }

  @Override
  public int hashCode() {
    return Objects.hash(device_id, app_store_id, bundle_id, event_name, usd_num_value, event_value, event_id,
            event_time, logging_time, attribution, os_version, device_model, device_language,
            carrier, city, user_ip, additional_product_data, idfv);
  }
}
