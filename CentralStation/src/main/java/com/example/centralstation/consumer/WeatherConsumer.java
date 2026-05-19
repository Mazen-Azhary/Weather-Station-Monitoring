package com.example.centralstation.consumer;

import com.example.centralstation.model.WeatherReading;
import com.example.centralstation.parquet.ParquetArchiver;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Reads weather messages from Kafka topic "weather-readings"
 * and sends each parsed record to the ParquetArchiver.
 */
public class WeatherConsumer implements Runnable {

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
        System.out.println("Weather consumer started.");
        try {
            while (running) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    try {
                        WeatherReading reading = mapper.readValue(record.value(), WeatherReading.class);
                        archiver.add(reading);
                    } catch (Exception e) {
                        System.err.println("Skipping bad message at offset "
                                + record.offset() + ": " + e.getMessage());
                    }
                }
            }
        } catch (WakeupException e) {
            // Expected on shutdown — not an error
        } finally {
            consumer.close();
            System.out.println("Weather consumer stopped.");
        }
    }

    /** Call this to stop the consumer loop cleanly. */
    public void stop() {
        running = false;
        consumer.wakeup(); // unblocks poll() immediately
    }
}
