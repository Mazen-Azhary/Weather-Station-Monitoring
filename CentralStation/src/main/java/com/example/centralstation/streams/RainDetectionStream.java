package com.example.centralstation.streams;

import com.example.centralstation.model.RainAlert;
import com.example.centralstation.model.WeatherReading;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

/**
 * Kafka Streams pipeline for rain detection.
 *
 * Pipeline:
 *   weather-readings → filter(humidity > 70) → publish alert → raining-alerts
 */
public class RainDetectionStream {

    private final KafkaStreams streams;

    public RainDetectionStream(String bootstrapServers, String inputTopic, String outputTopic) {
        ObjectMapper mapper = new ObjectMapper();

        StreamsBuilder builder = new StreamsBuilder();

        builder.<String, String>stream(inputTopic)
            // Step 1: Keep only messages where humidity > 70
            .filter((key, value) -> {
                try {
                    WeatherReading r = mapper.readValue(value, WeatherReading.class);
                    return r.getWeather() != null && r.getWeather().getHumidity() > 70;
                } catch (Exception e) {
                    return false; // skip malformed messages
                }
            })
            // Step 2: Build a RainAlert JSON message
            .mapValues(value -> {
                try {
                    WeatherReading r = mapper.readValue(value, WeatherReading.class);
                    RainAlert alert = new RainAlert(
                            r.getStationId(),
                            r.getWeather().getHumidity(),
                            r.getWeather().getTemperature(),
                            r.getWeather().getWindSpeed(),
                            r.getStatusTimestamp()
                    );
                    return mapper.writeValueAsString(alert);
                } catch (Exception e) {
                    return null;
                }
            })
            // Step 3: Drop nulls (failed conversions)
            .filter((key, value) -> value != null)
            // Step 4: Publish to raining-alerts
            .to(outputTopic);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,             "rain-detector");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,          bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Read from beginning so no messages are missed on startup
        props.put("auto.offset.reset", "earliest");

        this.streams = new KafkaStreams(builder.build(), props);
    }

    public void start() {
        streams.start();
        System.out.println("Rain detection stream started.");
    }

    public void stop() {
        streams.close();
        System.out.println("Rain detection stream stopped.");
    }
}
