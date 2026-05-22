package com.example.centralstation.consumer;

import com.example.centralstation.bitcask.Bitcask;
import com.example.centralstation.model.WeatherReading;
import com.example.centralstation.parquet.ParquetArchiver;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Reads weather messages from Kafka topic "weather-readings",
 * writes each to Parquet (via ParquetArchiver) and to the Bitcask
 * key-value store (latest reading per station).
 */
public class WeatherConsumer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(WeatherConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final ParquetArchiver archiver;
    private final ObjectMapper mapper = new ObjectMapper();
    private volatile boolean running = true;

    public WeatherConsumer(String bootstrapServers, String topic, ParquetArchiver archiver) {
        this.archiver = archiver;

        Properties props = new Properties();
        props.put("bootstrap.servers",  bootstrapServers);
        props.put("group.id",           "central-station-consumer");
        props.put("auto.offset.reset",  "earliest");
        props.put("key.deserializer",   "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(List.of(topic));
    }

    @Override
    public void run() {
        logger.info("Weather consumer started.");
        try {
            while (running) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    processRecord(record);
                }
            }
        } catch (WakeupException e) {
            // Expected on shutdown — not an error
        } finally {
            consumer.close();
            logger.info("Weather consumer stopped.");
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        WeatherReading reading;

        // 1. Parse JSON
        try {
            reading = mapper.readValue(record.value(), WeatherReading.class);
        } catch (Exception e) {
            logger.warn("Skipping bad message at offset {}: {}", record.offset(), e.getMessage());
            return;
        }

        // 2. Write to Parquet (batched)
        try {
            archiver.add(reading);
        } catch (Exception e) {
            logger.error("Parquet write failed for station {} at offset {}",
                    reading.getStationId(), record.offset(), e);
        }

        // 3. Write to Bitcask — latest reading per station (key = stationId)
        try {
            String key   = String.valueOf(reading.getStationId());
            String value = record.value();
            Bitcask.getInstance().write(key, value);
        } catch (Exception e) {
            // Log but never crash — Kafka consumption must continue
            logger.error("Bitcask write failed for station {} at offset {}",
                    reading.getStationId(), record.offset(), e);
        }
    }

    /** Call this to stop the consumer loop cleanly. */
    public void stop() {
        running = false;
        consumer.wakeup();
    }
}
