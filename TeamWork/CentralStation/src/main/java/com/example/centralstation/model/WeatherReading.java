package com.example.centralstation.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Domain model for the weather reading message published by Member 1's weather
 * stations.  The JSON field names MUST match exactly – do NOT rename them.
 *
 * Expected JSON:
 * {
 *   "station_id": 1,
 *   "s_no": 1,
 *   "battery_status": "low",
 *   "status_timestamp": 1681521224,
 *   "weather": {
 *     "humidity": 35,
 *     "temperature": 100,
 *     "wind_speed": 13
 *   }
 * }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WeatherReading {

    @JsonProperty("station_id")
    private long stationId;

    @JsonProperty("s_no")
    private long sequenceNumber;

    @JsonProperty("battery_status")
    private String batteryStatus;

    @JsonProperty("status_timestamp")
    private long statusTimestamp;

    @JsonProperty("weather")
    private WeatherData weather;

    // ------------------------------------------------------------------ //
    //  Constructors
    // ------------------------------------------------------------------ //

    public WeatherReading() {}

    public WeatherReading(long stationId, long sequenceNumber,
                          String batteryStatus, long statusTimestamp,
                          WeatherData weather) {
        this.stationId       = stationId;
        this.sequenceNumber  = sequenceNumber;
        this.batteryStatus   = batteryStatus;
        this.statusTimestamp = statusTimestamp;
        this.weather         = weather;
    }

    // ------------------------------------------------------------------ //
    //  Getters / Setters
    // ------------------------------------------------------------------ //

    public long getStationId()                  { return stationId; }
    public void setStationId(long v)            { this.stationId = v; }

    public long getSequenceNumber()             { return sequenceNumber; }
    public void setSequenceNumber(long v)       { this.sequenceNumber = v; }

    public String getBatteryStatus()            { return batteryStatus; }
    public void setBatteryStatus(String v)      { this.batteryStatus = v; }

    public long getStatusTimestamp()            { return statusTimestamp; }
    public void setStatusTimestamp(long v)      { this.statusTimestamp = v; }

    public WeatherData getWeather()             { return weather; }
    public void setWeather(WeatherData v)       { this.weather = v; }

    @Override
    public String toString() {
        return "WeatherReading{stationId=" + stationId
                + ", s_no=" + sequenceNumber
                + ", battery=" + batteryStatus
                + ", ts=" + statusTimestamp
                + ", weather=" + weather + '}';
    }

    // ================================================================== //
    //  Nested: WeatherData
    // ================================================================== //

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class WeatherData {

        @JsonProperty("humidity")
        private int humidity;

        @JsonProperty("temperature")
        private int temperature;

        @JsonProperty("wind_speed")
        private int windSpeed;

        public WeatherData() {}

        public WeatherData(int humidity, int temperature, int windSpeed) {
            this.humidity    = humidity;
            this.temperature = temperature;
            this.windSpeed   = windSpeed;
        }

        public int getHumidity()           { return humidity; }
        public void setHumidity(int v)     { this.humidity = v; }

        public int getTemperature()        { return temperature; }
        public void setTemperature(int v)  { this.temperature = v; }

        public int getWindSpeed()          { return windSpeed; }
        public void setWindSpeed(int v)    { this.windSpeed = v; }

        @Override
        public String toString() {
            return "WeatherData{humidity=" + humidity
                    + ", temperature=" + temperature
                    + ", windSpeed=" + windSpeed + '}';
        }
    }
}
