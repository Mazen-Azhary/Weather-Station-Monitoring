package com.example.centralstation.durable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitors the health of the WeatherConsumer by writing and checking heartbeat files.
 *
 * Durable Subscriber Pattern (Health Monitoring):
 * - Consumer writes heartbeats at regular intervals to a file
 * - If no heartbeat is detected within a timeout, consumer is considered DOWN
 * - External processes can detect consumer downtime and trigger replay logic
 *
 * Heartbeat file location: durable-subscriber/consumer-heartbeat.txt
 * Format: timestamp (milliseconds since epoch)
 */
public class ConsumerHealthMonitor {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerHealthMonitor.class);

    private final Path heartbeatFile;
    private final long heartbeatIntervalMs;  // How often consumer writes heartbeat
    private final long timeoutMs;            // How long without heartbeat = consumer DOWN

    private long lastHeartbeatTime = 0;
    private volatile boolean isRunning = false;

    /**
     * Create a health monitor for the consumer.
     *
     * @param storageDir Base directory for durable subscriber storage
     * @param heartbeatIntervalMs Interval (ms) at which consumer writes heartbeats (default: 5000)
     * @param timeoutMs Timeout (ms) to mark consumer as DOWN (default: 15000 = 3x heartbeat interval)
     */
    public ConsumerHealthMonitor(String storageDir, long heartbeatIntervalMs, long timeoutMs) {
        this.heartbeatFile = Paths.get(storageDir, "consumer-heartbeat.txt");
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.timeoutMs = timeoutMs;

        // Ensure storage directory exists
        try {
            Files.createDirectories(heartbeatFile.getParent());
        } catch (IOException e) {
            logger.error("Failed to create heartbeat directory: {}", e.getMessage());
        }
    }

    /**
     * Standard constructor: 5-second heartbeat interval, 15-second timeout (3x interval)
     */
    public ConsumerHealthMonitor(String storageDir) {
        this(storageDir, 5000, 15000);
    }

    /**
     * Start health monitoring. Should be called once at consumer startup.
     */
    public synchronized void start() {
        isRunning = true;
        lastHeartbeatTime = System.currentTimeMillis();
        recordHeartbeat();
        logger.info("Consumer health monitor started. Heartbeat interval: {}ms, Timeout: {}ms",
                heartbeatIntervalMs, timeoutMs);
    }

    /**
     * Stop health monitoring. Should be called at consumer shutdown.
     */
    public synchronized void stop() {
        isRunning = false;
        try {
            Files.deleteIfExists(heartbeatFile);
            logger.info("Consumer health monitor stopped. Heartbeat file removed.");
        } catch (IOException e) {
            logger.warn("Failed to remove heartbeat file: {}", e.getMessage());
        }
    }

    /**
     * Record a heartbeat. Consumer should call this at regular intervals.
     * Typically called every 5 seconds (configured by heartbeatIntervalMs).
     * Heartbeat is Base64 encoded for security.
     */
    public synchronized void recordHeartbeat() {
        if (!isRunning) {
            return;
        }
        try {
            long now = System.currentTimeMillis();
            lastHeartbeatTime = now;
            String plaintext = String.valueOf(now);
            String encoded = Base64.getEncoder().encodeToString(plaintext.getBytes(StandardCharsets.UTF_8));
            Files.writeString(heartbeatFile, encoded);
        } catch (IOException e) {
            logger.warn("Failed to write heartbeat: {}", e.getMessage());
        }
    }

    /**
     * Check if the consumer is currently healthy (recent heartbeat).
     * Returns false if no heartbeat or heartbeat is too old.
     * Heartbeat is Base64 encoded; decoded before checking.
     */
    public boolean isConsumerHealthy() {
        if (!Files.exists(heartbeatFile)) {
            return false;  // No heartbeat file = consumer never started
        }

        try {
            String encoded = Files.readString(heartbeatFile);
            byte[] decodedBytes = Base64.getDecoder().decode(encoded.trim());
            String plaintext = new String(decodedBytes, StandardCharsets.UTF_8);
            long lastHeartbeat = Long.parseLong(plaintext);
            long timeSinceHeartbeat = System.currentTimeMillis() - lastHeartbeat;

            boolean healthy = timeSinceHeartbeat < timeoutMs;
            if (!healthy) {
                logger.warn("Consumer is DOWN. Last heartbeat was {}ms ago (timeout: {}ms)",
                        timeSinceHeartbeat, timeoutMs);
            }
            return healthy;
        } catch (IOException | NumberFormatException | IllegalArgumentException e) {
            logger.warn("Failed to check consumer health: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Get the timestamp of the last heartbeat (ms since epoch).
     * Returns 0 if no heartbeat exists.
     * Heartbeat is Base64 encoded; decoded before returning.
     */
    public long getLastHeartbeatTime() {
        if (!Files.exists(heartbeatFile)) {
            return 0;
        }

        try {
            String encoded = Files.readString(heartbeatFile);
            byte[] decodedBytes = Base64.getDecoder().decode(encoded.trim());
            String plaintext = new String(decodedBytes, StandardCharsets.UTF_8);
            return Long.parseLong(plaintext);
        } catch (IOException | NumberFormatException | IllegalArgumentException e) {
            logger.warn("Failed to read last heartbeat: {}", e.getMessage());
            return 0;
        }
    }

    /**
     * Get time elapsed since last heartbeat (in milliseconds).
     */
    public long getTimeSinceLastHeartbeat() {
        long lastHeartbeat = getLastHeartbeatTime();
        if (lastHeartbeat == 0) {
            return Long.MAX_VALUE;  // No heartbeat = infinite time
        }
        return System.currentTimeMillis() - lastHeartbeat;
    }

    /**
     * Get the configured heartbeat interval.
     */
    public long getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    /**
     * Get the configured timeout.
     */
    public long getTimeoutMs() {
        return timeoutMs;
    }

    /**
     * Check if running.
     */
    public boolean isRunning() {
        return isRunning;
    }
}
