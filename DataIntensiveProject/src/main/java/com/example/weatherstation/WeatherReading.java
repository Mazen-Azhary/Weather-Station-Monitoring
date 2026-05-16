package com.example.weatherstation;

import com.fasterxml.jackson.annotation.JsonProperty;

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

    public WeatherReading() {
    }

    public WeatherReading(long stationId, long sequenceNumber,
            String batteryStatus, long statusTimestamp,
            WeatherData weather) {
        this.stationId = stationId;
        this.sequenceNumber = sequenceNumber;
        this.batteryStatus = batteryStatus;
        this.statusTimestamp = statusTimestamp;
        this.weather = weather;
    }

    public long getStationId() {
        return stationId;
    }

    public void setStationId(long v) {
        this.stationId = v;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(long v) {
        this.sequenceNumber = v;
    }

    public String getBatteryStatus() {
        return batteryStatus;
    }

    public void setBatteryStatus(String v) {
        this.batteryStatus = v;
    }

    public long getStatusTimestamp() {
        return statusTimestamp;
    }

    public void setStatusTimestamp(long v) {
        this.statusTimestamp = v;
    }

    public WeatherData getWeather() {
        return weather;
    }

    public void setWeather(WeatherData v) {
        this.weather = v;
    }

    public static class WeatherData {

        @JsonProperty("humidity")
        private int humidity;

        @JsonProperty("temperature")
        private int temperature;

        @JsonProperty("wind_speed")
        private int windSpeed;

        public WeatherData() {
        }

        public WeatherData(int humidity, int temperature, int windSpeed) {
            this.humidity = humidity;
            this.temperature = temperature;
            this.windSpeed = windSpeed;
        }

        public int getHumidity() {
            return humidity;
        }

        public void setHumidity(int v) {
            this.humidity = v;
        }

        public int getTemperature() {
            return temperature;
        }

        public void setTemperature(int v) {
            this.temperature = v;
        }

        public int getWindSpeed() {
            return windSpeed;
        }

        public void setWindSpeed(int v) {
            this.windSpeed = v;
        }
    }
}