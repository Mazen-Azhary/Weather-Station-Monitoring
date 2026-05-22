package com.example.centralstation.deadletter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Dead Letter Handler - writes failed messages to disk for archival and investigation.
 *
 * Dead Letter Channel Pattern: Routes messages that fail during processing to a durable store
 * separate from the main processing pipeline. Enables:
 *   - Monitoring: Identify systematic failures
 *   - Investigation: Diagnose why messages failed
 *   - Replay: Retry messages after fixing underlying issue
 *
 * Directory structure:
 *   dead-letters/
 *   ├── 2026-05-22/
 *   │   ├── PARQUET_WRITE/
 *   │   │   ├── 1715934000000_offset_1234.json
 *   │   │   └── ...
 *   │   ├── BITCASK_WRITE/
 *   │   │   ├── 1715934120000_offset_5678.json
 *   │   │   └── ...
 *   │   └── DUPLICATE_HANDLING/
 *   ├── 2026-05-21/
 *   └── ...
 */
public class DeadLetterHandler {

    private static final Logger logger = LoggerFactory.getLogger(DeadLetterHandler.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final Path deadLetterRoot;

    /**
     * @param deadLetterDirectory Root directory for storing dead letters (e.g., "dead-letters" or "/var/log/weather/dead-letters")
     */
    public DeadLetterHandler(String deadLetterDirectory) {
        this.deadLetterRoot = Paths.get(deadLetterDirectory);
        ensureDirectoryExists();
    }

    /**
     * Write a dead letter message to disk.
     * File is stored as: dead-letters/{DATE}/{STAGE}/{TIMESTAMP}_offset_{OFFSET}.json
     *
     * @param dlm The dead letter message to store
     * @return Path to the written file, or null if write failed
     */
    public Path writeDeadLetter(DeadLetterMessage dlm) {
        try {
            // Determine directory path: dead-letters/{DATE}/{STAGE}/
            String stage = dlm.getProcessingStage() != null ? dlm.getProcessingStage() : "UNKNOWN";
            String dateStr = LocalDate.now().format(dateFormatter);

            Path stageDir = deadLetterRoot
                    .resolve(dateStr)
                    .resolve(stage);

            Files.createDirectories(stageDir);

            // Create filename: {TIMESTAMP}_offset_{OFFSET}.json
            long timestamp = dlm.getFailedAtTimestamp();
            long offset = dlm.getKafkaOffset();
            String filename = String.format("%d_offset_%d.json", timestamp, offset);
            Path filePath = stageDir.resolve(filename);

            // Serialize to JSON and write
            String jsonContent = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(dlm);
            Files.writeString(filePath, jsonContent);

            logger.warn("Dead letter written: {} [{}:{}]", filePath, stage, offset);
            return filePath;

        } catch (IOException e) {
            logger.error("Failed to write dead letter message: {}", dlm, e);
            return null;
        }
    }

    /**
     * Get all dead letters for a specific date and stage.
     *
     * @param date Date in format "yyyy-MM-dd" (e.g., "2026-05-22")
     * @param stage Processing stage (e.g., "PARQUET_WRITE", "BITCASK_WRITE", "DUPLICATE_HANDLING")
     * @return List of dead letter messages, or empty list if none found
     */
    public List<DeadLetterMessage> readDeadLetters(String date, String stage) {
        try {
            Path stageDir = deadLetterRoot.resolve(date).resolve(stage);

            if (!Files.exists(stageDir)) {
                logger.info("No dead letters found for date={} stage={}", date, stage);
                return List.of();
            }

            return Files.list(stageDir)
                    .filter(p -> p.toString().endsWith(".json"))
                    .map(this::readSingleDeadLetter)
                    .filter(dlm -> dlm != null)
                    .toList();

        } catch (IOException e) {
            logger.error("Error reading dead letters for date={} stage={}", date, stage, e);
            return List.of();
        }
    }

    /**
     * Get all dead letter files for a specific date, across all stages.
     *
     * @param date Date in format "yyyy-MM-dd"
     * @return List of all dead letter messages for that date
     */
    public List<DeadLetterMessage> readDeadLettersByDate(String date) {
        try {
            Path dateDir = deadLetterRoot.resolve(date);

            if (!Files.exists(dateDir)) {
                logger.info("No dead letters found for date={}", date);
                return List.of();
            }

            return Files.walk(dateDir)
                    .filter(p -> p.toString().endsWith(".json"))
                    .map(this::readSingleDeadLetter)
                    .filter(dlm -> dlm != null)
                    .toList();

        } catch (IOException e) {
            logger.error("Error reading dead letters for date={}", date, e);
            return List.of();
        }
    }

    /**
     * Get all dead letters for a specific station across all dates/stages.
     *
     * @param stationId Station ID to filter by
     * @return List of dead letters for that station
     */
    public List<DeadLetterMessage> readDeadLettersByStation(long stationId) {
        try {
            if (!Files.exists(deadLetterRoot)) {
                return List.of();
            }

            return Files.walk(deadLetterRoot)
                    .filter(p -> p.toString().endsWith(".json"))
                    .map(this::readSingleDeadLetter)
                    .filter(dlm -> dlm != null && dlm.getStationId() != null && dlm.getStationId() == stationId)
                    .toList();

        } catch (IOException e) {
            logger.error("Error reading dead letters for station={}", stationId, e);
            return List.of();
        }
    }

    /**
     * Get stats: count of dead letters by stage.
     *
     * @param date Date in format "yyyy-MM-dd"
     * @return String with formatted statistics
     */
    public String getStatistics(String date) {
        try {
            Path dateDir = deadLetterRoot.resolve(date);

            if (!Files.exists(dateDir)) {
                return "No dead letters for date: " + date;
            }

            StringBuilder sb = new StringBuilder();
            sb.append("Dead Letter Statistics for ").append(date).append("\n");
            sb.append("========================================\n");

            Files.list(dateDir)
                    .filter(Files::isDirectory)
                    .forEach(stageDir -> {
                        try {
                            String stage = stageDir.getFileName().toString();
                            long count = Files.list(stageDir)
                                    .filter(p -> p.toString().endsWith(".json"))
                                    .count();
                            sb.append(stage).append(": ").append(count).append(" messages\n");
                        } catch (IOException e) {
                            logger.error("Error counting stage directory", e);
                        }
                    });

            return sb.toString();

        } catch (IOException e) {
            logger.error("Error reading statistics for date={}", date, e);
            return "Error reading statistics: " + e.getMessage();
        }
    }

    /**
     * Delete a specific dead letter file (for maintenance).
     *
     * @param filePath Path to the dead letter file
     * @return true if successfully deleted
     */
    public boolean deleteDeadLetter(Path filePath) {
        try {
            boolean deleted = Files.deleteIfExists(filePath);
            if (deleted) {
                logger.info("Deleted dead letter: {}", filePath);
            }
            return deleted;
        } catch (IOException e) {
            logger.error("Error deleting dead letter: {}", filePath, e);
            return false;
        }
    }

    // Private helper to read a single dead letter file
    private DeadLetterMessage readSingleDeadLetter(Path filePath) {
        try {
            return mapper.readValue(Files.readAllBytes(filePath), DeadLetterMessage.class);
        } catch (IOException e) {
            logger.error("Error reading dead letter file: {}", filePath, e);
            return null;
        }
    }

    // Ensure root directory exists
    private void ensureDirectoryExists() {
        try {
            Files.createDirectories(deadLetterRoot);
            logger.info("Dead letter storage directory ensured: {}", deadLetterRoot.toAbsolutePath());
        } catch (IOException e) {
            logger.error("Failed to create dead letter directory: {}", deadLetterRoot, e);
        }
    }

    /**
     * @return The root path where dead letters are stored
     */
    public Path getRootPath() {
        return deadLetterRoot;
    }
}
