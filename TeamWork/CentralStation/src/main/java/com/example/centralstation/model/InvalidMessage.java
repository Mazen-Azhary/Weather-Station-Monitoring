package com.example.centralstation.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a weather message that failed validation.
 * Part of the Invalid Message Channel pattern — routes business-rule violations
 * to a dedicated Kafka topic for monitoring and debugging.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class InvalidMessage {

    @JsonProperty("original_message")
    private String originalMessage;

    @JsonProperty("validation_errors")
    private List<String> validationErrors;

    @JsonProperty("rejected_at_timestamp")
    private long rejectedAtTimestamp;

    @JsonProperty("kafka_offset")
    private long kafkaOffset;

    @JsonProperty("kafka_partition")
    private int kafkaPartition;

    // ------------------------------------------------------------------ //
    //  Constructors
    // ------------------------------------------------------------------ //

    public InvalidMessage() {}

    public InvalidMessage(String originalMessage, List<String> validationErrors,
                          long rejectedAtTimestamp, long kafkaOffset, int kafkaPartition) {
        this.originalMessage = originalMessage;
        this.validationErrors = validationErrors;
        this.rejectedAtTimestamp = rejectedAtTimestamp;
        this.kafkaOffset = kafkaOffset;
        this.kafkaPartition = kafkaPartition;
    }

    // ------------------------------------------------------------------ //
    //  Getters / Setters
    // ------------------------------------------------------------------ //

    public String getOriginalMessage() { return originalMessage; }
    public void setOriginalMessage(String v) { this.originalMessage = v; }

    public List<String> getValidationErrors() { return validationErrors; }
    public void setValidationErrors(List<String> v) { this.validationErrors = v; }

    public long getRejectedAtTimestamp() { return rejectedAtTimestamp; }
    public void setRejectedAtTimestamp(long v) { this.rejectedAtTimestamp = v; }

    public long getKafkaOffset() { return kafkaOffset; }
    public void setKafkaOffset(long v) { this.kafkaOffset = v; }

    public int getKafkaPartition() { return kafkaPartition; }
    public void setKafkaPartition(int v) { this.kafkaPartition = v; }

    @Override
    public String toString() {
        return "InvalidMessage{" +
                "originalMessage='" + originalMessage + '\'' +
                ", validationErrors=" + validationErrors +
                ", rejectedAtTimestamp=" + rejectedAtTimestamp +
                ", kafkaOffset=" + kafkaOffset +
                ", kafkaPartition=" + kafkaPartition +
                '}';
    }
}
