package com.example.centralstation.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Rain-alert message published to the {@code raining-alerts} Kafka topic
 * whenever a weather reading reports humidity > 70 %.
 *
 * Example output:
 * {
 *   "station_id": 3,
 *   "alert_type": "RAIN_DETECTED",
 *   "humidity": 82,
 *   "temperature": 26,
 *   "wind_speed": 12,
 *   "status_timestamp": 1681521350
 * }
 */
public class RainAlert {

    @JsonProperty("station_id")
    private long stationId;

    @JsonProperty("alert_type")
    private String alertType = "RAIN_DETECTED";

    @JsonProperty("humidity")
    private int humidity;

    @JsonProperty("temperature")
    private int temperature;

    @JsonProperty("wind_speed")
    private int windSpeed;

    @JsonProperty("status_timestamp")
    private long statusTimestamp;

    // ------------------------------------------------------------------ //
    //  Constructors
    // ------------------------------------------------------------------ //

    public RainAlert() {}

    public RainAlert(long stationId, int humidity, int temperature,
                     int windSpeed, long statusTimestamp) {
        this.stationId       = stationId;
        this.alertType       = "RAIN_DETECTED";
        this.humidity        = humidity;
        this.temperature     = temperature;
        this.windSpeed       = windSpeed;
        this.statusTimestamp = statusTimestamp;
    }

    // ------------------------------------------------------------------ //
    //  Getters / Setters
    // ------------------------------------------------------------------ //

    public long getStationId()                   { return stationId; }
    public void setStationId(long v)             { this.stationId = v; }

    public String getAlertType()                 { return alertType; }
    public void setAlertType(String v)           { this.alertType = v; }

    public int getHumidity()                     { return humidity; }
    public void setHumidity(int v)               { this.humidity = v; }

    public int getTemperature()                  { return temperature; }
    public void setTemperature(int v)            { this.temperature = v; }

    public int getWindSpeed()                    { return windSpeed; }
    public void setWindSpeed(int v)              { this.windSpeed = v; }

    public long getStatusTimestamp()             { return statusTimestamp; }
    public void setStatusTimestamp(long v)       { this.statusTimestamp = v; }

    @Override
    public String toString() {
        return "RainAlert{stationId=" + stationId
                + ", alertType=" + alertType
                + ", humidity=" + humidity
                + ", temperature=" + temperature
                + ", windSpeed=" + windSpeed
                + ", ts=" + statusTimestamp + '}';
    }
}
