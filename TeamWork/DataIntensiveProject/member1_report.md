# Weather Monitoring System - Member 1 Report

# 1. Architecture Overview
For my part of the Data Intensive Applications project, I designed and implemented the Data Acquisition layer. This layer acts as the foundation of the weather monitoring system, consisting of 10 independent weather stations that continuously emit their readings to a central Kafka cluster.

I built the stations using a Java-based application that leverages the `kafka-clients` library to publish records. The application can run in two modes:
1. Simulated Data Mode - uses a mock data generator with built-in randomization for realistic weather conditions.
2. Open-Meteo Adapter Mode - acts as a Channel Adapter that fetches live, real-world data from the Open-Meteo API.

# 2. Setup Instructions
To run the Data Acquisition layer, you need Docker and Docker Compose installed.

1. Set the Data Source Toggle (Optional)
   The system defaults to using open-meteo data. If you wish to simulate fake data, set the `STATION_TYPE` environment variable in your terminal:
   
   Command: export STATION_TYPE="simulated"
   
   If you don't set this variable, all 10 stations will run in `open-meteo` mode.

2. Start the Cluster**
   
   Command: docker-compose up
   
   This will spin up Zookeeper, Kafka, and the 10 weather stations in their respective containers.

# 3. Weather Station Mock Implementation
In the simulated mode, each station thread outputs a JSON message every second. To ensure the simulated data mirrors realistic edge cases, I implemented the following logic:

- Battery Status Distribution - I used randomization thresholds to achieve the required distribution: 30% `low`, 40% `medium`, and 30% `high`.
- Packet Loss Simulation - I introduced a 10% message drop rate. When a message is dropped, the auto-incrementing sequence number (`s_no`) still increments, which will help Member 4 visualize dropped messages later in Kibana.
- Weather Data - Generated realistic random boundaries for humidity (0-100%), temperature (30-120°F), and wind speed (0-150 km/h).

# 4. Kafka Producer Integration
The stations connect to the Kafka broker (`kafka:9092`) and publish to the `weather-readings` topic. 
I ensured high reliability by configuring:
- `acks=all` so that records are safely committed.
- `enable.idempotence=true` to prevent duplicates in case of network retries.
- Meaningful application logs using `SLF4J` and `Logback`. Both the simulated and Open-Meteo adapters now log the exact JSON payload being sent, making it incredibly easy to debug and verify that the data structure is correct.

# 5. Dockerization
I packaged the Java application into a Docker image named `weather-station:latest` using a multi-stage Dockerfile:
1. Build Stage - Uses `maven:3.9.6-eclipse-temurin-17` to fetch dependencies offline and build the executable JAR cleanly.
2. Run Stage - Uses a lightweight `eclipse-temurin:17-jre-jammy` image.
I added a `HEALTHCHECK` to ensure the process stays alive. Inside `docker-compose.yml`, all 10 stations use this exact same image but are injected with different `STATION_ID` environment variables.

# 6. Open-Meteo Channel Adapter (Bonus)
As part of the Enterprise Integration Patterns bonus, I implemented a Channel Adapter pattern using the Open-Meteo API.
When the `STATION_TYPE` is set to `open-meteo`, the station skips the simulated generation and instead makes an HTTP GET request to Open-Meteo. It parses the resulting JSON tree to extract real-time `temperature_2m`, `relative_humidity_2m`, and `wind_speed_10m`, and then formats them into our exact `weather-readings` JSON schema before pushing to Kafka. This allows the rest of the downstream architecture (Bitcask and Parquet) to process real data seamlessly without noticing any difference.

# 7. Source code

1. WeatherReading.java - Data structure for weather readings

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

2. WeatherStation.java - Weather station application

package com.example.weatherstation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.weatherstation.WeatherReading;
import com.example.weatherstation.WeatherReading.WeatherData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class WeatherStation implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(WeatherStation.class);

    public static final String TOPIC = "weather-readings";

    private static final double LOW_THRESHOLD = 0.30;
    private static final double MEDIUM_THRESHOLD = 0.70;
    private static final double DROP_RATE = 0.10;

    private static final int HUMIDITY_MIN = 0;
    private static final int HUMIDITY_MAX = 100;
    private static final int TEMPERATURE_MIN = 30;
    private static final int TEMPERATURE_MAX = 120;
    private static final int WIND_SPEED_MIN = 0;
    private static final int WIND_SPEED_MAX = 150;

    private final long stationId;
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;
    private final Random random;
    private final AtomicLong sequenceCounter = new AtomicLong(1);

    private volatile boolean running = true;

    public WeatherStation(long stationId, String bootstrapServers) {
        this.stationId = stationId;
        this.objectMapper = new ObjectMapper();
        this.random = new Random();
        this.producer = buildProducer(bootstrapServers);
    }

    private KafkaProducer<String, String> buildProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        return new KafkaProducer<>(props);
    }

    @Override
    public void run() {
        log.info("Station {} started → publishing to topic '{}'", stationId, TOPIC);

        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                tick();
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Station {} encountered an error: {}", stationId, e.getMessage(), e);
            }
        }

        producer.close();
        log.info("Station {} stopped.", stationId);
    }

    private void tick() throws Exception {

        if (random.nextDouble() < DROP_RATE) {
            sequenceCounter.incrementAndGet();
            log.debug("Station {} dropped a message (simulated packet loss)", stationId);
            return;
        }

        WeatherReading reading = buildReading();
        String json = objectMapper.writeValueAsString(reading);

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, String.valueOf(stationId), json);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Station {} failed to send message s_no={}: {}",
                        stationId, reading.getSequenceNumber(), exception.getMessage());
            } else {
                log.info("Station {} → partition={} offset={} s_no={} reading={}",
                        stationId, metadata.partition(), metadata.offset(),
                        reading.getSequenceNumber(), json);
            }
        });
    }

    private WeatherReading buildReading() {
        long sNo = sequenceCounter.getAndIncrement();
        String battery = pickBatteryStatus();
        long timestamp = System.currentTimeMillis() / 1_000L;

        WeatherData weather = new WeatherData(
                randomInt(HUMIDITY_MIN, HUMIDITY_MAX),
                randomInt(TEMPERATURE_MIN, TEMPERATURE_MAX),
                randomInt(WIND_SPEED_MIN, WIND_SPEED_MAX));

        return new WeatherReading(stationId, sNo, battery, timestamp, weather);
    }

    private String pickBatteryStatus() {
        double p = random.nextDouble();
        if (p < LOW_THRESHOLD)
            return "low";
        if (p < MEDIUM_THRESHOLD)
            return "medium";
        return "high";
    }

    private int randomInt(int min, int max) {
        return min + random.nextInt(max - min + 1);
    }

    public void stop() {
        this.running = false;
    }
}

3. WeatherStationApp.java - Main entry point

package com.example.weatherstation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class WeatherStationApp {

    private static final Logger log = LoggerFactory.getLogger(WeatherStationApp.class);

    public static void main(String[] args) throws InterruptedException {

        long stationId = longEnv("STATION_ID", 1L);
        String bootstrapServers = strEnv("KAFKA_BOOTSTRAP", "kafka:9092");
        String stationType = strEnv("STATION_TYPE", "simulated");

        log.info("=== Weather Station Application ===");
        log.info("STATION_ID      : {}", stationId);
        log.info("KAFKA_BOOTSTRAP : {}", bootstrapServers);
        log.info("STATION_TYPE    : {}", stationType);

        List<Runnable> stations = new ArrayList<>();
        ExecutorService executor = Executors.newCachedThreadPool();

        if ("open-meteo".equalsIgnoreCase(stationType)) {
            OpenMeteoAdapter adapter = new OpenMeteoAdapter(stationId, bootstrapServers);
            stations.add(adapter);
            executor.submit(adapter);
            log.info("Launched Open-Meteo adapter for station {}", stationId);
        } else {
            WeatherStation ws = new WeatherStation(stationId, bootstrapServers);
            stations.add(ws);
            executor.submit(ws);
            log.info("Launched simulated weather station {}", stationId);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received – stopping all stations …");
            for (Runnable r : stations) {
                if (r instanceof WeatherStation)
                    ((WeatherStation) r).stop();
                else if (r instanceof OpenMeteoAdapter)
                    ((OpenMeteoAdapter) r).stop();
            }
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            log.info("All stations stopped. Goodbye.");
        }));

        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }

    private static String strEnv(String key, String defaultValue) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }

    private static long longEnv(String key, long defaultValue) {
        try {
            return Long.parseLong(strEnv(key, String.valueOf(defaultValue)));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

}

4. OpenMeteoAdapter.java - Open-Meteo real adapter

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

# 8. Docker files

1. Dockerfile - containerize everything

FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /build

COPY pom.xml .
RUN mvn dependency:go-offline -q

COPY src ./src
RUN mvn clean package -DskipTests -q

FROM eclipse-temurin:17-jre-jammy

LABEL maintainer="Weather Monitoring Project"
LABEL description="IoT Weather Station – publishes readings to Kafka"

WORKDIR /app

COPY --from=builder /build/target/DataIntensiveProject-1.0-SNAPSHOT.jar app.jar

ENV STATION_ID=1
ENV KAFKA_BOOTSTRAP=kafka:9092

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD pgrep -f "app.jar" > /dev/null || exit 1

ENTRYPOINT ["java", "-jar", "app.jar"]

2. docker-compose.yml - Orchestrate everything

version: "3.9"

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - weather-net

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_NUM_PARTITIONS: 10
    depends_on:
      - zookeeper
    networks:
      - weather-net
    healthcheck:
      test: [ "CMD", "kafka-topics", "--bootstrap-server", "kafka:9092", "--list" ]
      interval: 15s
      timeout: 10s
      retries: 5

  station-1:
    image: weather-station:latest
    container_name: station-1
    environment:
      STATION_ID: 1
      KAFKA_BOOTSTRAP: kafka:9092
      STATION_TYPE: ${STATION_TYPE:-open-meteo}
    depends_on:
      kafka:
        condition: service_healthy
    restart: on-failure
    networks:
      - weather-net

  station-2:
    image: weather-station:latest
    container_name: station-2
    environment:
      STATION_ID: 2
      KAFKA_BOOTSTRAP: kafka:9092
      STATION_TYPE: ${STATION_TYPE:-open-meteo}
    depends_on:
      kafka:
        condition: service_healthy
    restart: on-failure
    networks:
      - weather-net

  station-3:
    image: weather-station:latest
    container_name: station-3
    environment:
      STATION_ID: 3
      KAFKA_BOOTSTRAP: kafka:9092
      STATION_TYPE: ${STATION_TYPE:-open-meteo}
    depends_on:
      kafka:
        condition: service_healthy
    restart: on-failure
    networks:
      - weather-net

  station-4:
    image: weather-station:latest
    container_name: station-4
    environment:
      STATION_ID: 4
      KAFKA_BOOTSTRAP: kafka:9092
      STATION_TYPE: ${STATION_TYPE:-open-meteo}
    depends_on:
      kafka:
        condition: service_healthy
    restart: on-failure
    networks:
      - weather-net

  station-5:
    image: weather-station:latest
    container_name: station-5
    environment:
      STATION_ID: 5
      KAFKA_BOOTSTRAP: kafka:9092
      STATION_TYPE: ${STATION_TYPE:-open-meteo}
    depends_on:
      kafka:
        condition: service_healthy
    restart: on-failure
    networks:
      - weather-net

  station-6:
    image: weather-station:latest
    container_name: station-6
    environment:
      STATION_ID: 6
      KAFKA_BOOTSTRAP: kafka:9092
      STATION_TYPE: ${STATION_TYPE:-open-meteo}
    depends_on:
      kafka:
        condition: service_healthy
    restart: on-failure
    networks:
      - weather-net

  station-7:
    image: weather-station:latest
    container_name: station-7
    environment:
      STATION_ID: 7
      KAFKA_BOOTSTRAP: kafka:9092
      STATION_TYPE: ${STATION_TYPE:-open-meteo}
    depends_on:
      kafka:
        condition: service_healthy
    restart: on-failure
    networks:
      - weather-net

  station-8:
    image: weather-station:latest
    container_name: station-8
    environment:
      STATION_ID: 8
      KAFKA_BOOTSTRAP: kafka:9092
      STATION_TYPE: ${STATION_TYPE:-open-meteo}
    depends_on:
      kafka:
        condition: service_healthy
    restart: on-failure
    networks:
      - weather-net

  station-9:
    image: weather-station:latest
    container_name: station-9
    environment:
      STATION_ID: 9
      KAFKA_BOOTSTRAP: kafka:9092
      STATION_TYPE: ${STATION_TYPE:-open-meteo}
    depends_on:
      kafka:
        condition: service_healthy
    restart: on-failure
    networks:
      - weather-net

  station-10:
    image: weather-station:latest
    container_name: station-10
    environment:
      STATION_ID: 10
      KAFKA_BOOTSTRAP: kafka:9092
      STATION_TYPE: ${STATION_TYPE:-open-meteo}
    depends_on:
      kafka:
        condition: service_healthy
    restart: on-failure
    networks:
      - weather-net

networks:
  weather-net:
    driver: bridge
