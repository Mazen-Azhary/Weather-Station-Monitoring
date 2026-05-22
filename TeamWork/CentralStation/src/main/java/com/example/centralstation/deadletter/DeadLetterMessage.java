package com.example.centralstation.deadletter;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a message that failed during processing and was routed to the Dead Letter Channel.
 * These are valid, non-duplicate messages that failed at the infrastructure level (I/O, network, etc).
 *
 * Dead Letter Channel Pattern: Routes messages that fail after validation to a separate channel
 * for investigation and potential replay.
 */
public class DeadLetterMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("original_message")
    private String originalMessage;

    @JsonProperty("failure_reasons")
    private List<String> failureReasons;

    @JsonProperty("failed_at_timestamp_ms")
    private long failedAtTimestamp;

    @JsonProperty("kafka_offset")
    private long kafkaOffset;

    @JsonProperty("kafka_partition")
    private int kafkaPartition;

    @JsonProperty("processing_stage")
    private String processingStage;  // e.g., "PARQUET_WRITE", "BITCASK_WRITE"

    @JsonProperty("exception_type")
    private String exceptionType;

    @JsonProperty("exception_message")
    private String exceptionMessage;

    @JsonProperty("station_id")
    private Long stationId;  // Extracted if parseable

    @JsonProperty("sequence_number")
    private Long sequenceNumber;  // Extracted if parseable

    // For JPA/Jackson
    public DeadLetterMessage() {
    }

    public DeadLetterMessage(String originalMessage, List<String> failureReasons,
                            long kafkaOffset, int kafkaPartition, String processingStage,
                            Exception exception, Long stationId, Long sequenceNumber) {
        this.originalMessage = originalMessage;
        this.failureReasons = failureReasons;
        this.failedAtTimestamp = System.currentTimeMillis();
        this.kafkaOffset = kafkaOffset;
        this.kafkaPartition = kafkaPartition;
        this.processingStage = processingStage;
        this.exceptionType = exception != null ? exception.getClass().getSimpleName() : null;
        this.exceptionMessage = exception != null ? exception.getMessage() : null;
        this.stationId = stationId;
        this.sequenceNumber = sequenceNumber;
    }

    // Getters
    public String getOriginalMessage() {
        return originalMessage;
    }

    public List<String> getFailureReasons() {
        return failureReasons;
    }

    public long getFailedAtTimestamp() {
        return failedAtTimestamp;
    }

    public long getKafkaOffset() {
        return kafkaOffset;
    }

    public int getKafkaPartition() {
        return kafkaPartition;
    }

    public String getProcessingStage() {
        return processingStage;
    }

    public String getExceptionType() {
        return exceptionType;
    }

    public String getExceptionMessage() {
        return exceptionMessage;
    }

    public Long getStationId() {
        return stationId;
    }

    public Long getSequenceNumber() {
        return sequenceNumber;
    }

    // Setters (for JSON deserialization)
    public void setOriginalMessage(String originalMessage) {
        this.originalMessage = originalMessage;
    }

    public void setFailureReasons(List<String> failureReasons) {
        this.failureReasons = failureReasons;
    }

    public void setFailedAtTimestamp(long failedAtTimestamp) {
        this.failedAtTimestamp = failedAtTimestamp;
    }

    public void setKafkaOffset(long kafkaOffset) {
        this.kafkaOffset = kafkaOffset;
    }

    public void setKafkaPartition(int kafkaPartition) {
        this.kafkaPartition = kafkaPartition;
    }

    public void setProcessingStage(String processingStage) {
        this.processingStage = processingStage;
    }

    public void setExceptionType(String exceptionType) {
        this.exceptionType = exceptionType;
    }

    public void setExceptionMessage(String exceptionMessage) {
        this.exceptionMessage = exceptionMessage;
    }

    public void setStationId(Long stationId) {
        this.stationId = stationId;
    }

    public void setSequenceNumber(Long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public String toString() {
        return "DeadLetterMessage{" +
                "stageFailed='" + processingStage + '\'' +
                ", offset=" + kafkaOffset +
                ", exception=" + exceptionType +
                ", station=" + stationId +
                ", timestamp=" + failedAtTimestamp +
                '}';
    }
}
