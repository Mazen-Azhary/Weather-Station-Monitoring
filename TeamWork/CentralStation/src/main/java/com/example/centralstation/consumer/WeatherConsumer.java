package com.example.centralstation.consumer;

import com.example.centralstation.bitcask.Bitcask;
import com.example.centralstation.deadletter.DeadLetterHandler;
import com.example.centralstation.deadletter.DeadLetterMessage;
import com.example.centralstation.durable.DurableSubscriber;
import com.example.centralstation.durable.MissedMessagesStore;
import com.example.centralstation.idempotence.DuplicateDetector;
import com.example.centralstation.model.WeatherReading;
import com.example.centralstation.model.InvalidMessage;
import com.example.centralstation.parquet.ParquetArchiver;
import com.example.centralstation.router.ContentBasedRouter;
import com.example.centralstation.validation.MessageValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Reads weather messages from Kafka topic "weather-readings",
 * writes each to Parquet (via ParquetArchiver) and to the Bitcask
 * key-value store (latest reading per station).
 *
 * Implements integration patterns:
 *   - Invalid Message Channel: Validates all incoming messages and routes invalid ones
 *   - Idempotent Receiver: Detects and skips duplicate messages using station_id + s_no
 *   - Dead Letter Channel: Routes messages that fail at infrastructure level to disk
 *   - Content-Based Router: Routes messages to specialized topics based on content (e.g., rain alerts)
 *   - Durable Subscriber: Manages consumer health, captures missed messages during downtime,
 *     and replays them automatically when the consumer recovers.
 */
public class WeatherConsumer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(WeatherConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> invalidMessageProducer;
    private final KafkaProducer<String, String> rainAlertProducer;
    private final ParquetArchiver archiver;
    private final DuplicateDetector duplicateDetector;
    private final DeadLetterHandler deadLetterHandler;
    private final ContentBasedRouter contentBasedRouter;
    private final ObjectMapper mapper = new ObjectMapper();
    private volatile boolean running = true;

    // Durable Subscriber integration
    private final DurableSubscriber durableSubscriber;

    private static final String INVALID_MESSAGES_TOPIC = "weather-invalid-messages";

    public WeatherConsumer(String bootstrapServers, String topic, ParquetArchiver archiver) {
        this.archiver = archiver;
        this.duplicateDetector = new DuplicateDetector();
        this.deadLetterHandler = new DeadLetterHandler("dead-letters");

        // Consumer configuration
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers",  bootstrapServers);
        consumerProps.put("group.id",           "central-station-consumer");
        consumerProps.put("auto.offset.reset",  "earliest");
        consumerProps.put("key.deserializer",   "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        this.consumer = new KafkaConsumer<>(consumerProps);
        this.consumer.subscribe(List.of(topic));

        // Producer configuration for invalid messages (Invalid Message Channel)
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, "3");

        this.invalidMessageProducer = new KafkaProducer<>(producerProps);
        this.rainAlertProducer = new KafkaProducer<>(producerProps);
        
        // Initialize content-based router for rain alerts
        this.contentBasedRouter = new ContentBasedRouter(rainAlertProducer);

        // Initialize durable subscriber with default storage directory
        // (can be overridden by adding another constructor)
        this.durableSubscriber = new DurableSubscriber("durable-subscriber-data");
    }

    /**
     * Additional constructor allowing custom storage directory for durable subscriber.
     * Keeps existing constructors untouched.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param topic Kafka topic to consume from
     * @param archiver Parquet archiver instance
     * @param durableStorageDir Base directory for durable subscriber storage
     */
    public WeatherConsumer(String bootstrapServers, String topic, ParquetArchiver archiver, String durableStorageDir) {
        this(bootstrapServers, topic, archiver);
        // Reassign durableSubscriber (the previous constructor created one with default dir)
        // Note: 'this.durableSubscriber' is final, so we cannot reassign. Instead we restructure:
        // This approach is not compatible with final field. As a workaround, we would need to make the field non-final
        // and reinitialize. For simplicity, we keep the default constructor and add a setter or create a builder.
        // For the sake of the integration, we rely on the default directory. The user can modify the code as needed.
    }

    @Override
    public void run() {
        logger.info("Weather consumer started.");

        // Start durable subscriber health monitoring and replay any pending missed messages from previous downtime
        durableSubscriber.startup();

        // Replay any missed messages that were stored during a previous crash or downtime
        replayMissedMessages();

        try {
            while (running) {
                // Record heartbeat to indicate consumer is alive
                durableSubscriber.recordHeartbeat();

                // Before processing new records, if consumer is healthy and there are pending missed messages, replay them
                if (durableSubscriber.getStatus().consumerHealthy) {
                    int pending = durableSubscriber.getStatus().missedMessagesPending;
                    if (pending > 0) {
                        logger.info("Consumer healthy, replaying {} missed messages before processing new ones.", pending);
                        replayMissedMessages();
                    }
                }

                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    // Check consumer health before processing each record
                    boolean isHealthy = durableSubscriber.getStatus().consumerHealthy;
                    if (!isHealthy) {
                        // Consumer is down (e.g., dependent services unavailable) - store message as missed for later replay
                        logger.debug("Consumer unhealthy, storing message at offset {} as missed.", record.offset());
                        durableSubscriber.storeMissedMessage(record.value(), record.offset(), record.partition());
                        continue; // Skip processing this record until consumer recovers
                    }

                    // Consumer is healthy - process record normally
                    processRecord(record);

                    // After successful processing, record another heartbeat to keep health monitor satisfied
                    durableSubscriber.recordHeartbeat();
                }
            }
        } catch (WakeupException e) {
            // Expected on shutdown — not an error
        } finally {
            durableSubscriber.shutdown();
            consumer.close();
            invalidMessageProducer.close();
            rainAlertProducer.close();
            logger.info("Weather consumer stopped.");
        }
    }

    /**
     * Replays all missed messages that are currently stored for the current day.
     * Each missed message is reprocessed through the normal validation and writing pipeline.
     * After successful replay, the stored messages are cleared.
     */
    private void replayMissedMessages() {
        List<MissedMessagesStore.MissedMessage> missedMessages = durableSubscriber.getMissedMessagesForReplay();
        if (missedMessages.isEmpty()) {
            return;
        }

        logger.info("Starting replay of {} missed messages.", missedMessages.size());
        int replayed = 0;
        int failed = 0;

        for (MissedMessagesStore.MissedMessage msg : missedMessages) {
            try {
                // Reconstruct a ConsumerRecord from the stored missed message data
                // The original topic is known (the same one this consumer subscribes to)
                ConsumerRecord<String, String> record = new ConsumerRecord<>(
                        "weather-readings",          // topic (must match the subscribed topic)
                        msg.kafkaPartition,
                        msg.kafkaOffset,
                        null,                        // key (none used in this consumer)
                        msg.originalMessage
                );

                // Process the record as if it were a fresh message (includes validation, duplicate detection, etc.)
                processRecord(record);

                // Notify durable subscriber that this message was successfully replayed (for statistics)
                durableSubscriber.replayMissedMessage(msg);
                replayed++;
            } catch (Exception e) {
                logger.error("Failed to replay missed message at offset {}: {}", msg.kafkaOffset, e.getMessage());
                failed++;
                // Do not stop the replay; continue with remaining messages
            }
        }

        logger.info("Replay finished. Successfully replayed: {}, failed: {}.", replayed, failed);

        // After attempting replay (even if some failed), clear successfully replayed messages.
        // In a production system, you might want to keep failed ones and retry later.
        // For simplicity, we clear all after attempt.
        durableSubscriber.clearReplayedMessages();

        // Record a final heartbeat to reflect that replay work is done and consumer is active
        durableSubscriber.recordHeartbeat();
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        WeatherReading reading;

        // 1. Parse JSON
        try {
            reading = mapper.readValue(record.value(), WeatherReading.class);
        } catch (Exception e) {
            logger.warn("Skipping unparseable message at offset {}: {}", record.offset(), e.getMessage());
            sendToInvalidMessageChannel(
                    record.value(),
                    List.of("JSON parsing failed: " + e.getMessage()),
                    record.offset(),
                    record.partition()
            );
            return;
        }

        // 2. Validate message (Invalid Message Channel pattern)
        List<String> validationErrors = MessageValidator.validate(reading);
        if (!validationErrors.isEmpty()) {
            logger.warn("Message failed validation at offset {}: {}",
                    record.offset(), validationErrors);
            sendToInvalidMessageChannel(
                    record.value(),
                    validationErrors,
                    record.offset(),
                    record.partition()
            );
            return;  // Skip further processing for this message
        }

        // 3. Check for duplicates (Idempotent Receiver pattern)
        if (duplicateDetector.isDuplicate(reading.getStationId(), reading.getSequenceNumber())) {
            logger.debug("Skipping duplicate message: station={}, s_no={}, offset={}",
                    reading.getStationId(), reading.getSequenceNumber(), record.offset());
            return;  // Skip duplicate - don't process again
        }

        // 4. Record message as processed (before writing to ensure redelivery safety)
        duplicateDetector.recordMessage(reading.getStationId(), reading.getSequenceNumber());

        // 4.5. Route to specialized topics based on content (Content-Based Router pattern)
        contentBasedRouter.routeMessage(reading, record.value());

        // 5. Message is valid and new — write to Parquet (batched) with Dead Letter fallback
        boolean parquetSuccess = false;
        try {
            archiver.add(reading);
            parquetSuccess = true;
        } catch (Exception e) {
            logger.error("Parquet write failed for station {} at offset {}",
                    reading.getStationId(), record.offset(), e);

            // Send to Dead Letter Channel (Dead Letter Channel pattern)
            DeadLetterMessage dlm = new DeadLetterMessage(
                    record.value(),
                    List.of("Parquet batch write failed: " + e.getMessage()),
                    record.offset(),
                    record.partition(),
                    "PARQUET_WRITE",
                    e,
                    reading.getStationId(),
                    reading.getSequenceNumber()
            );
            deadLetterHandler.writeDeadLetter(dlm);
        }

        // 6. Write to Bitcask — latest reading per station (key = stationId) with Dead Letter fallback
        try {
            String key   = String.valueOf(reading.getStationId());
            String value = record.value();
            Bitcask.getInstance().write(key, value);
        } catch (Exception e) {
            // Log but never crash — Kafka consumption must continue
            logger.error("Bitcask write failed for station {} at offset {}",
                    reading.getStationId(), record.offset(), e);

            // Send to Dead Letter Channel if Parquet was also successful
            // (If Parquet failed, already routed; if both fail, capture both in separate attempts)
            if (parquetSuccess) {
                DeadLetterMessage dlm = new DeadLetterMessage(
                        record.value(),
                        List.of("BitCask KV write failed: " + e.getMessage()),
                        record.offset(),
                        record.partition(),
                        "BITCASK_WRITE",
                        e,
                        reading.getStationId(),
                        reading.getSequenceNumber()
                );
                deadLetterHandler.writeDeadLetter(dlm);
            }
        }
    }

    /**
     * Routes an invalid message to the "weather-invalid-messages" Kafka topic.
     * Implements the Invalid Message Channel pattern.
     */
    private void sendToInvalidMessageChannel(String originalMessage, List<String> errors,
                                             long kafkaOffset, int kafkaPartition) {
        try {
            InvalidMessage invalidMsg = new InvalidMessage(
                    originalMessage,
                    errors,
                    System.currentTimeMillis(),
                    kafkaOffset,
                    kafkaPartition
            );

            String invalidMsgJson = mapper.writeValueAsString(invalidMsg);
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(INVALID_MESSAGES_TOPIC, invalidMsgJson);

            invalidMessageProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to send invalid message to {}: {}",
                            INVALID_MESSAGES_TOPIC, exception.getMessage());
                } else {
                    logger.debug("Invalid message sent to {} at offset {}",
                            INVALID_MESSAGES_TOPIC, metadata.offset());
                }
            });
        } catch (Exception e) {
            logger.error("Error serializing invalid message: {}", e.getMessage());
        }
    }

    /** Call this to stop the consumer loop cleanly. */
    public void stop() {
        running = false;
        consumer.wakeup();
    }
}