package com.example.centralstation;

import com.example.centralstation.consumer.WeatherConsumer;
import com.example.centralstation.parquet.ParquetArchiver;
import com.example.centralstation.streams.RainDetectionStream;

/**
 * Central Station — Main entry point.
 *
 * Starts three things in the same JVM:
 *   1. ParquetArchiver   — batches and writes weather data to Parquet files
 *   2. RainDetectionStream — Kafka Streams that detects humidity > 70
 *   3. WeatherConsumer   — reads from weather-readings topic
 */
public class CentralStationApplication {

    // ---- Configuration (read from environment variables) ----
    static final String KAFKA     = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    static final String IN_TOPIC  = env("WEATHER_TOPIC",           "weather-readings");
    static final String OUT_TOPIC = env("RAIN_ALERT_TOPIC",        "raining-alerts");
    static final String DATA_PATH = env("PARQUET_OUTPUT_PATH",     "/app/data/parquet");
    static final int    BATCH     = 10_000;

    public static void main(String[] args) throws InterruptedException {

        // Required on Windows: tells Hadoop where its home dir is so it
        // does not try to load winutils.exe or native libraries
        System.setProperty("hadoop.home.dir",
                System.getProperty("java.io.tmpdir"));

        ParquetArchiver archiver = new ParquetArchiver(DATA_PATH, BATCH);
        archiver.start();

        // 2. Kafka Streams rain detector (runs on its own thread pool)
        RainDetectionStream rainStream = new RainDetectionStream(KAFKA, IN_TOPIC, OUT_TOPIC);
        rainStream.start();

        // 3. Kafka consumer (runs on a dedicated thread)
        WeatherConsumer consumer = new WeatherConsumer(KAFKA, IN_TOPIC, archiver);
        Thread consumerThread = new Thread(consumer, "consumer-thread");
        consumerThread.start();

        // Graceful shutdown on Ctrl+C or Docker stop
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            consumer.stop();
            rainStream.stop();
            archiver.stop();
            System.out.println("Shutdown complete.");
        }, "shutdown-hook"));

        System.out.println("Central Station running. Listening on topic: " + IN_TOPIC);
        consumerThread.join(); // keep main alive
    }

    static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }
}
