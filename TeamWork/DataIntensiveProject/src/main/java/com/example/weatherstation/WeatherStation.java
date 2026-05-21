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