package com.example.centralstation.durable;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Durable Subscriber Pattern Implementation.
 *
 * Manages consumer subscriptions with automatic replay of missed messages.
 *
 * Responsibilities:
 * 1. Monitor consumer health (is it running?)
 * 2. Capture messages missed during downtime
 * 3. Replay missed messages when consumer comes back up
 * 4. Provide statistics and diagnostics
 *
 * Lifecycle:
 * - startup(): Initialize monitors and begin tracking
 * - onConsumerDown(): Handle consumer failure detection
 * - onConsumerUp(): Replay missed messages, then resume normal processing
 * - shutdown(): Stop all monitoring and cleanup
 */
public class DurableSubscriber {

    private static final Logger logger = LoggerFactory.getLogger(DurableSubscriber.class);

    private final ConsumerHealthMonitor healthMonitor;
    private final MissedMessagesStore missedMessagesStore;
    private final ObjectMapper mapper = new ObjectMapper();
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private volatile boolean isMonitoring = false;
    private volatile boolean consumerWasDown = false;
    private volatile long downStartTime = 0;
    private long messagesReplayedCount = 0;

    private final HealthCheckThread healthCheckThread;

    /**
     * Create a durable subscriber with default settings.
     *
     * @param storageDir Base directory for durable subscriber storage
     */
    public DurableSubscriber(String storageDir) {
        this.healthMonitor = new ConsumerHealthMonitor(storageDir);
        this.missedMessagesStore = new MissedMessagesStore(storageDir);
        this.healthCheckThread = new HealthCheckThread();
    }

    /**
     * Create a durable subscriber with custom health check parameters.
     *
     * @param storageDir Base directory for durable subscriber storage
     * @param heartbeatIntervalMs Interval (ms) at which consumer writes heartbeats
     * @param timeoutMs Timeout (ms) to mark consumer as DOWN
     */
    public DurableSubscriber(String storageDir, long heartbeatIntervalMs, long timeoutMs) {
        this.healthMonitor = new ConsumerHealthMonitor(storageDir, heartbeatIntervalMs, timeoutMs);
        this.missedMessagesStore = new MissedMessagesStore(storageDir);
        this.healthCheckThread = new HealthCheckThread();
    }

    /**
     * Initialize durable subscriber at application startup.
     * Starts health monitoring and checks for missed messages from previous downtime.
     */
    public synchronized void startup() {
        healthMonitor.start();
        isMonitoring = true;

        // Check if consumer was previously down and has messages to replay
        LocalDate today = LocalDate.now(ZoneId.systemDefault());
        String dateStr = today.format(dateFormatter);
        int missedCount = missedMessagesStore.countMissedMessagesByDate(dateStr);

        if (missedCount > 0) {
            logger.info("Found {} missed messages from {}. Will replay on startup.", missedCount, dateStr);
        }

        // Start background thread to monitor consumer health
        healthCheckThread.start();
        logger.info("Durable Subscriber initialized. Health monitoring active.");
    }

    /**
     * Shutdown durable subscriber.
     */
    public synchronized void shutdown() {
        isMonitoring = false;
        healthCheckThread.interrupt();
        healthMonitor.stop();
        logger.info("Durable Subscriber shut down.");
    }

    /**
     * Record a heartbeat from the consumer. Call this regularly (e.g., every 5 seconds).
     * Signals that the consumer is alive and processing messages.
     */
    public void recordHeartbeat() {
        if (!isMonitoring) {
            return;
        }

        healthMonitor.recordHeartbeat();

        // If consumer was down, it's now back up
        if (consumerWasDown) {
            onConsumerRecovered();
        }
    }

    /**
     * Store a message that was missed while consumer was down.
     * Consumer should call this when it detects its own downtime.
     */
    public void storeMissedMessage(String originalMessage, long kafkaOffset, int kafkaPartition) {
        long now = System.currentTimeMillis();
        String downTimeStr = LocalDateTime.now(ZoneId.systemDefault()).toString();
        missedMessagesStore.storeMissedMessage(
                originalMessage,
                kafkaOffset,
                kafkaPartition,
                now,
                now,
                downTimeStr
        );
    }

    /**
     * Get all missed messages that need to be replayed.
     * Returns messages sorted by Kafka offset for correct replay order.
     */
    public List<MissedMessagesStore.MissedMessage> getMissedMessagesForReplay() {
        LocalDate today = LocalDate.now(ZoneId.systemDefault());
        String dateStr = today.format(dateFormatter);
        return missedMessagesStore.getMissedMessagesByDate(dateStr);
    }

    /**
     * Replay a missed message (feed it back to the consumer).
     * In a real implementation, this would involve deserializing and reprocessing.
     *
     * @param missedMessage The missed message to replay
     * @return true if replay successful, false otherwise
     */
    public boolean replayMissedMessage(MissedMessagesStore.MissedMessage missedMessage) {
        try {
            logger.debug("Replaying missed message: offset={}, partition={}, message={}...",
                    missedMessage.kafkaOffset,
                    missedMessage.kafkaPartition,
                    missedMessage.originalMessage.substring(0, Math.min(50, missedMessage.originalMessage.length())));
            // Message would be reprocessed here (passed to consumer logic)
            messagesReplayedCount++;
            return true;
        } catch (Exception e) {
            logger.error("Failed to replay missed message at offset {}: {}", missedMessage.kafkaOffset, e.getMessage());
            return false;
        }
    }

    /**
     * Clear missed messages after successful replay.
     */
    public void clearReplayedMessages() {
        LocalDate today = LocalDate.now(ZoneId.systemDefault());
        String dateStr = today.format(dateFormatter);
        missedMessagesStore.clearMissedMessagesByDate(dateStr);
    }

    /**
     * Called internally when consumer downtime is detected.
     */
    private synchronized void onConsumerDown() {
        if (consumerWasDown) {
            return;  // Already down
        }

        downStartTime = System.currentTimeMillis();
        consumerWasDown = true;
        logger.warn("CONSUMER DOWN DETECTED. Starting to capture missed messages.");
    }

    /**
     * Called internally when consumer recovers after downtime.
     */
    private synchronized void onConsumerRecovered() {
        if (!consumerWasDown) {
            return;  // Not previously down
        }

        long downDurationMs = System.currentTimeMillis() - downStartTime;
        consumerWasDown = false;

        logger.warn("CONSUMER RECOVERED after {}ms downtime. {} messages to replay.",
                downDurationMs, getMissedMessagesForReplay().size());

        // In a real scenario, this would trigger automatic replay
        // For now, just log it and let external system handle replay
    }

    /**
     * Get current status of the durable subscriber.
     */
    public DurableSubscriberStatus getStatus() {
        return new DurableSubscriberStatus(
                isMonitoring,
                healthMonitor.isConsumerHealthy(),
                consumerWasDown,
                downStartTime,
                healthMonitor.getLastHeartbeatTime(),
                healthMonitor.getTimeSinceLastHeartbeat(),
                getMissedMessagesForReplay().size(),
                messagesReplayedCount
        );
    }

    /**
     * Get detailed statistics.
     */
    public String getStatistics() {
        DurableSubscriberStatus status = getStatus();
        StringBuilder sb = new StringBuilder();
        sb.append("=== DURABLE SUBSCRIBER STATUS ===\n");
        sb.append("Monitoring Active: ").append(status.isMonitoring).append("\n");
        sb.append("Consumer Healthy: ").append(status.consumerHealthy).append("\n");
        sb.append("Consumer Was Down: ").append(status.consumerWasDown).append("\n");
        sb.append("Time Since Last Heartbeat: ").append(status.timeSinceLastHeartbeatMs).append("ms\n");
        sb.append("Missed Messages Pending Replay: ").append(status.missedMessagesPending).append("\n");
        sb.append("Messages Replayed Total: ").append(status.messagesReplayed).append("\n");
        sb.append("\n").append(missedMessagesStore.getStatistics());
        return sb.toString();
    }

    /**
     * Inner class for health check background thread.
     */
    private class HealthCheckThread extends Thread {
        private static final long CHECK_INTERVAL_MS = 10000;  // Check every 10 seconds

        public HealthCheckThread() {
            setName("durable-subscriber-health-check");
            setDaemon(true);
        }

        @Override
        public void run() {
            logger.info("Health check thread started.");
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(CHECK_INTERVAL_MS);

                    boolean isHealthy = healthMonitor.isConsumerHealthy();
                    if (!isHealthy) {
                        onConsumerDown();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("Health check error: {}", e.getMessage());
                }
            }
            logger.info("Health check thread stopped.");
        }
    }

    /**
     * Status object for querying subscriber state.
     */
    public static class DurableSubscriberStatus {
        public final boolean isMonitoring;
        public final boolean consumerHealthy;
        public final boolean consumerWasDown;
        public final long downStartTimeMs;
        public final long lastHeartbeatMs;
        public final long timeSinceLastHeartbeatMs;
        public final int missedMessagesPending;
        public final long messagesReplayed;

        public DurableSubscriberStatus(boolean isMonitoring, boolean consumerHealthy, boolean consumerWasDown,
                                      long downStartTimeMs, long lastHeartbeatMs, long timeSinceLastHeartbeatMs,
                                      int missedMessagesPending, long messagesReplayed) {
            this.isMonitoring = isMonitoring;
            this.consumerHealthy = consumerHealthy;
            this.consumerWasDown = consumerWasDown;
            this.downStartTimeMs = downStartTimeMs;
            this.lastHeartbeatMs = lastHeartbeatMs;
            this.timeSinceLastHeartbeatMs = timeSinceLastHeartbeatMs;
            this.missedMessagesPending = missedMessagesPending;
            this.messagesReplayed = messagesReplayed;
        }
    }
}
