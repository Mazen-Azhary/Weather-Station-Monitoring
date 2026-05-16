package com.example.weatherstation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class OpenMeteoAdapter implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(OpenMeteoAdapter.class);

    private static final String TOPIC = "weather-readings";
    private static final String OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast?latitude=31.2001&longitude=29.9187&current=temperature_2m,relative_humidity_2m,wind_speed_10m&temperature_unit=fahrenheit";

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private final AtomicLong sequenceCounter = new AtomicLong(1);
    private final long stationId;

    private volatile boolean running = true;

    public OpenMeteoAdapter(long stationId, String bootstrapServers) {
        this.stationId = stationId;
        this.objectMapper = new ObjectMapper();
        this.httpClient = HttpClient.newBuilder().build();

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void run() {
        log.info("OpenMeteoAdapter started -> fetching from Open-Meteo and publishing to '{}'", TOPIC);

        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                fetchAndPublish();
                Thread.sleep(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("OpenMeteoAdapter encountered an error: {}", e.getMessage(), e);
            }
        }

        producer.close();
        log.info("OpenMeteoAdapter stopped.");
    }

    private void fetchAndPublish() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(OPEN_METEO_URL))
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 200) {
            JsonNode root = objectMapper.readTree(response.body());
            JsonNode current = root.path("current");

            int temperature = current.path("temperature_2m").asInt();
            int humidity = current.path("relative_humidity_2m").asInt();
            int windSpeed = current.path("wind_speed_10m").asInt();

            WeatherReading.WeatherData weatherData = new WeatherReading.WeatherData(humidity, temperature, windSpeed);

            long timestamp = System.currentTimeMillis() / 1_000L;
            long sNo = sequenceCounter.getAndIncrement();

            WeatherReading reading = new WeatherReading(stationId, sNo, "high", timestamp, weatherData);

            String json = objectMapper.writeValueAsString(reading);
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, String.valueOf(stationId), json);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("OpenMeteoAdapter failed to send message s_no={}: {}", sNo, exception.getMessage());
                } else {
                    log.info("OpenMeteoAdapter -> partition={} offset={} s_no={} reading={}", metadata.partition(),
                            metadata.offset(), sNo, json);
                }
            });
        } else {
            log.error("Failed to fetch from Open-Meteo. Status code: {}", response.statusCode());
        }
    }

    public void stop() {
        this.running = false;
    }
}
