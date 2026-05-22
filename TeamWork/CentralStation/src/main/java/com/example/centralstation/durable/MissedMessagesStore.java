package com.example.centralstation.durable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Stores messages that arrive while the consumer is down.
 *
 * Durable Subscriber Pattern (Message Storage):
 * - Captures messages missed during consumer downtime
 * - Organized by date in directory: durable-subscriber/missed-messages/{date}/
 * - Each message stored as JSON file: {timestamp}_{offset}.json
 * - File format includes original message, Kafka offset, timestamp, and metadata
 *
 * When consumer comes back up, messages are replayed in order.
 */
public class MissedMessagesStore {

    private static final Logger logger = LoggerFactory.getLogger(MissedMessagesStore.class);

    private final Path missedMessagesRoot;
    private final ObjectMapper mapper = new ObjectMapper();
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /**
     * Model for missed message entries.
     */
    public static class MissedMessage {
        @JsonProperty
        public String originalMessage;
        @JsonProperty
        public long kafkaOffset;
        @JsonProperty
        public int kafkaPartition;
        @JsonProperty
        public long messageTimestamp;
        @JsonProperty
        public long missedAtTimestamp;
        @JsonProperty
        public String downDetectionTime;

        // Jackson requires no-arg constructor
        public MissedMessage() {}

        public MissedMessage(String originalMessage, long kafkaOffset, int kafkaPartition,
                           long messageTimestamp, long missedAtTimestamp, String downDetectionTime) {
            this.originalMessage = originalMessage;
            this.kafkaOffset = kafkaOffset;
            this.kafkaPartition = kafkaPartition;
            this.messageTimestamp = messageTimestamp;
            this.missedAtTimestamp = missedAtTimestamp;
            this.downDetectionTime = downDetectionTime;
        }
    }

    /**
     * Create a missed messages store.
     *
     * @param storageDir Base directory for durable subscriber storage
     */
    public MissedMessagesStore(String storageDir) {
        this.missedMessagesRoot = Paths.get(storageDir, "missed-messages");
        try {
            Files.createDirectories(missedMessagesRoot);
        } catch (IOException e) {
            logger.error("Failed to create missed-messages directory: {}", e.getMessage());
        }
    }

    /**
     * Store a message that arrived while consumer was down.
     *
     * @param originalMessage The raw message JSON from Kafka
     * @param kafkaOffset The Kafka offset
     * @param kafkaPartition The Kafka partition
     * @param messageTimestamp When the message was created (ms)
     * @param missedAtTimestamp When this message was detected as missed (ms)
     * @param downDetectionTime Human-readable timestamp of downtime detection
     */
    public synchronized void storeMissedMessage(String originalMessage, long kafkaOffset, int kafkaPartition,
                                               long messageTimestamp, long missedAtTimestamp, String downDetectionTime) {
        try {
            LocalDate date = LocalDateTime.now(ZoneId.systemDefault()).toLocalDate();
            Path dateDir = missedMessagesRoot.resolve(date.format(dateFormatter));
            Files.createDirectories(dateDir);

            String filename = missedAtTimestamp + "_offset_" + kafkaOffset + ".json";
            Path filePath = dateDir.resolve(filename);

            MissedMessage missedMsg = new MissedMessage(
                    originalMessage,
                    kafkaOffset,
                    kafkaPartition,
                    messageTimestamp,
                    missedAtTimestamp,
                    downDetectionTime
            );

            String jsonContent = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(missedMsg);
            String encoded = Base64.getEncoder().encodeToString(jsonContent.getBytes(StandardCharsets.UTF_8));
            Files.writeString(filePath, encoded);

            logger.debug("Missed message stored: offset={}, date={}, file={}", kafkaOffset, date, filename);
        } catch (IOException e) {
            logger.error("Failed to store missed message: {}", e.getMessage());
        }
    }

    /**
     * Retrieve all missed messages from a specific date.
     *
     * @param dateStr Date in format "yyyy-MM-dd"
     * @return List of missed messages, sorted by offset (ascending)
     */
    public synchronized List<MissedMessage> getMissedMessagesByDate(String dateStr) {
        Path dateDir = missedMessagesRoot.resolve(dateStr);
        if (!Files.exists(dateDir)) {
            return new ArrayList<>();
        }

        try {
            return Files.list(dateDir)
                    .filter(p -> p.toString().endsWith(".json"))
                    .sorted()  // Sorted by filename = sorted by timestamp
                    .map(this::readMissedMessage)
                    .filter(msg -> msg != null)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            logger.error("Failed to read missed messages from {}: {}", dateStr, e.getMessage());
            return new ArrayList<>();
        }
    }

    /**
     * Retrieve all missed messages since a given timestamp.
     * Useful for replay when consumer was down for multiple days.
     *
     * @param sinceTimestamp Only include messages with missedAtTimestamp >= this value
     * @return List of missed messages, sorted by offset (ascending)
     */
    public synchronized List<MissedMessage> getMissedMessagesSince(long sinceTimestamp) {
        List<MissedMessage> allMessages = new ArrayList<>();
        try {
            if (!Files.exists(missedMessagesRoot)) {
                return allMessages;
            }

            Files.list(missedMessagesRoot)
                    .filter(Files::isDirectory)
                    .forEach(dateDir -> {
                        try {
                            Files.list(dateDir)
                                    .filter(p -> p.toString().endsWith(".json"))
                                    .map(this::readMissedMessage)
                                    .filter(msg -> msg != null && msg.missedAtTimestamp >= sinceTimestamp)
                                    .forEach(allMessages::add);
                        } catch (IOException e) {
                            logger.warn("Error reading date directory {}: {}", dateDir, e.getMessage());
                        }
                    });

            // Sort all messages by offset for replay order
            allMessages.sort((a, b) -> Long.compare(a.kafkaOffset, b.kafkaOffset));
        } catch (IOException e) {
            logger.error("Failed to read missed messages since {}: {}", sinceTimestamp, e.getMessage());
        }

        return allMessages;
    }

    /**
     * Count missed messages by date.
     */
    public synchronized int countMissedMessagesByDate(String dateStr) {
        return getMissedMessagesByDate(dateStr).size();
    }

    /**
     * Clear missed messages from a specific date.
     * Typically called after messages have been replayed.
     */
    public synchronized int clearMissedMessagesByDate(String dateStr) {
        Path dateDir = missedMessagesRoot.resolve(dateStr);
        if (!Files.exists(dateDir)) {
            return 0;
        }

        int count = 0;
        try {
            List<Path> files = Files.list(dateDir)
                    .filter(p -> p.toString().endsWith(".json"))
                    .collect(Collectors.toList());

            for (Path file : files) {
                Files.delete(file);
                count++;
            }

            // Remove empty date directory
            Files.deleteIfExists(dateDir);
            logger.info("Cleared {} missed messages from date {}", count, dateStr);
        } catch (IOException e) {
            logger.error("Failed to clear missed messages from {}: {}", dateStr, e.getMessage());
        }

        return count;
    }

    /**
     * Get statistics on missed messages.
     */
    public synchronized String getStatistics() {
        if (!Files.exists(missedMessagesRoot)) {
            return "No missed messages stored.";
        }

        try {
            int totalDates = (int) Files.list(missedMessagesRoot)
                    .filter(Files::isDirectory)
                    .count();

            List<String> dates = Files.list(missedMessagesRoot)
                    .filter(Files::isDirectory)
                    .map(p -> p.getFileName().toString())
                    .sorted()
                    .collect(Collectors.toList());

            StringBuilder sb = new StringBuilder();
            sb.append("Missed Messages Statistics:\n");
            sb.append("Total dates with missed messages: ").append(totalDates).append("\n");
            for (String date : dates) {
                int count = countMissedMessagesByDate(date);
                sb.append("  ").append(date).append(": ").append(count).append(" messages\n");
            }
            return sb.toString();
        } catch (IOException e) {
            logger.error("Failed to get statistics: {}", e.getMessage());
            return "Error retrieving statistics.";
        }
    }

    /**
     * Read a single missed message from JSON file.
     * Message is Base64 encoded; decoded before deserialization.
     */
    private MissedMessage readMissedMessage(Path filePath) {
        try {
            String encoded = Files.readString(filePath);
            byte[] decodedBytes = Base64.getDecoder().decode(encoded.trim());
            String jsonContent = new String(decodedBytes, StandardCharsets.UTF_8);
            return mapper.readValue(jsonContent, MissedMessage.class);
        } catch (IOException | IllegalArgumentException e) {
            logger.warn("Failed to read missed message file {}: {}", filePath, e.getMessage());
            return null;
        }
    }

    /**
     * Get the root storage directory.
     */
    public Path getRootDirectory() {
        return missedMessagesRoot;
    }
}
