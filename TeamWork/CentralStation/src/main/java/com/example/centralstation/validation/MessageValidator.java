package com.example.centralstation.validation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.example.centralstation.model.WeatherReading;

/**
 * Validates weather readings against business rules.
 * Implements the Invalid Message Channel pattern — collects validation errors
 * without throwing exceptions, allowing invalid messages to be routed separately.
 */
public class MessageValidator {

    private static final Set<String> VALID_BATTERY_STATUSES = new HashSet<>();

    static {
        VALID_BATTERY_STATUSES.add("low");
        VALID_BATTERY_STATUSES.add("medium");
        VALID_BATTERY_STATUSES.add("high");
    }

    // Weather bounds (Fahrenheit)
    private static final int MIN_TEMPERATURE = -50;
    private static final int MAX_TEMPERATURE = 150;

    private static final int MIN_HUMIDITY = 0;
    private static final int MAX_HUMIDITY = 100;

    private static final int MIN_WIND_SPEED = 0;

    /**
     * Validates a weather reading. Returns a list of validation errors.
     * Empty list means the reading is valid.
     *
     * @param reading The weather reading to validate
     * @return List of validation error messages (empty if valid)
     */
    public static List<String> validate(WeatherReading reading) {
        List<String> errors = new ArrayList<>();

        // Null checks
        if (reading == null) {
            errors.add("WeatherReading is null");
            return errors;
        }

        // 1. Validate station_id
        if (reading.getStationId() <= 0) {
            errors.add("station_id must be positive, got: " + reading.getStationId());
        }

        // 2. Validate sequence_number
        if (reading.getSequenceNumber() < 0) {
            errors.add("s_no must be non-negative, got: " + reading.getSequenceNumber());
        }

        // 3. Validate battery_status
        String batteryStatus = reading.getBatteryStatus();
        if (batteryStatus == null || batteryStatus.isBlank()) {
            errors.add("battery_status is null or empty");
        } else if (!VALID_BATTERY_STATUSES.contains(batteryStatus.toLowerCase())) {
            errors.add("battery_status must be one of [low, medium, high], got: '" + batteryStatus + "'");
        }

        // 4. Validate status_timestamp
        if (reading.getStatusTimestamp() <= 0) {
            errors.add("status_timestamp must be positive, got: " + reading.getStatusTimestamp());
        }

        // 5. Validate weather data
        WeatherReading.WeatherData weather = reading.getWeather();
        if (weather == null) {
            errors.add("weather data is null");
        } else {
            // Validate humidity
            if (weather.getHumidity() < MIN_HUMIDITY || weather.getHumidity() > MAX_HUMIDITY) {
                errors.add("humidity must be between " + MIN_HUMIDITY + " and " + MAX_HUMIDITY
                        + ", got: " + weather.getHumidity());
            }

            // Validate temperature
            if (weather.getTemperature() < MIN_TEMPERATURE || weather.getTemperature() > MAX_TEMPERATURE) {
                errors.add("temperature must be between " + MIN_TEMPERATURE + "°F and " + MAX_TEMPERATURE + "°F"
                        + ", got: " + weather.getTemperature() + "°F");
            }

            // Validate wind_speed
            if (weather.getWindSpeed() < MIN_WIND_SPEED) {
                errors.add("wind_speed must be non-negative (km/h), got: " + weather.getWindSpeed());
            }
        }

        return errors;
    }

    /**
     * Convenience method to check if a reading is valid.
     *
     * @param reading The weather reading to validate
     * @return true if valid (no errors), false otherwise
     */
    public static boolean isValid(WeatherReading reading) {
        return validate(reading).isEmpty();
    }
}
