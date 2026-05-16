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