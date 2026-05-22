package com.example.centralstation.idempotence;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Detects and tracks duplicate messages using a composite key (station_id + s_no).
 *
 * Implements the Idempotent Receiver pattern — ensures that processing
 * the same message multiple times does not corrupt the system's final state.
 *
 * Thread-safe implementation using ConcurrentHashMap for high-throughput scenarios.
 */
public class DuplicateDetector {

    /**
     * Tracks processed messages: key = "stationId:sequenceNumber", value = timestamp
     * Using ConcurrentHashMap for lock-free thread safety in high-throughput scenarios
     */
    private final ConcurrentHashMap<String, Long> processedMessages;

    /**
     * Maximum size of the duplicate detection cache.
     * Once exceeded, oldest entries are evicted (LRU-like behavior through external management)
     * Configurable based on memory constraints
     */
    private final int maxCacheSize;

    /**
     * Write lock for cache eviction operations (infrequent)
     */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Constructor with default cache size of 100,000 entries
     * Suitable for 10 weather stations sending messages continuously
     */
    public DuplicateDetector() {
        this(100_000);
    }

    /**
     * Constructor with custom cache size
     *
     * @param maxCacheSize Maximum number of entries to track
     */
    public DuplicateDetector(int maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
        this.processedMessages = new ConcurrentHashMap<>(maxCacheSize);
    }

    /**
     * Checks if a message has been processed before (Idempotent Receiver pattern).
     *
     * @param stationId Station ID from the message
     * @param sequenceNumber Sequence number from the message
     * @return true if this is a DUPLICATE (already processed), false if FIRST occurrence
     */
    public boolean isDuplicate(long stationId, long sequenceNumber) {
        String key = buildKey(stationId, sequenceNumber);
        return processedMessages.containsKey(key);
    }

    /**
     * Records a message as processed.
     * Call this ONLY after successfully validating but BEFORE writing to Parquet/BitCask
     * to ensure duplicate detection in case of redelivery.
     *
     * @param stationId Station ID from the message
     * @param sequenceNumber Sequence number from the message
     * @return true if this is a NEW message (was not previously recorded), 
     *         false if it was already recorded (duplicate)
     */
    public boolean recordMessage(long stationId, long sequenceNumber) {
        String key = buildKey(stationId, sequenceNumber);
        Long previousValue = processedMessages.putIfAbsent(key, System.currentTimeMillis());

        // Check if cache is getting full and needs cleanup
        if (processedMessages.size() > maxCacheSize) {
            evictOldestEntries();
        }

        return previousValue == null; // true if new (NULL before), false if duplicate (value existed)
    }

    /**
     * Removes a message from the tracking cache.
     * Useful for memory management when messages are older than retention period.
     *
     * @param stationId Station ID from the message
     * @param sequenceNumber Sequence number from the message
     */
    public void forgetMessage(long stationId, long sequenceNumber) {
        String key = buildKey(stationId, sequenceNumber);
        processedMessages.remove(key);
    }

    /**
     * Clears all tracked messages from the cache.
     * Use carefully — only when you're sure no duplicates can occur.
     */
    public void clearAll() {
        lock.writeLock().lock();
        try {
            processedMessages.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Gets the current number of tracked messages in the cache.
     *
     * @return Size of the tracking cache
     */
    public int getCacheSize() {
        return processedMessages.size();
    }

    /**
     * Gets the maximum cache size.
     *
     * @return Maximum cache size setting
     */
    public int getMaxCacheSize() {
        return maxCacheSize;
    }

    /**
     * Checks if a specific message exists in the cache.
     *
     * @param stationId Station ID
     * @param sequenceNumber Sequence number
     * @return true if message is tracked, false otherwise
     */
    public boolean isTracked(long stationId, long sequenceNumber) {
        String key = buildKey(stationId, sequenceNumber);
        return processedMessages.containsKey(key);
    }

    /**
     * Gets the timestamp when a message was first recorded.
     *
     * @param stationId Station ID
     * @param sequenceNumber Sequence number
     * @return Millisecond timestamp, or null if not tracked
     */
    public Long getRecordedTimestamp(long stationId, long sequenceNumber) {
        String key = buildKey(stationId, sequenceNumber);
        return processedMessages.get(key);
    }

    /**
     * Removes entries older than a specified timestamp.
     * Useful for periodic cleanup of old entries.
     *
     * @param beforeTimestamp Remove entries recorded before this timestamp (ms)
     * @return Number of entries removed
     */
    public int removeEntriesOlderThan(long beforeTimestamp) {
        lock.writeLock().lock();
        try {
            int removed = 0;
            for (String key : processedMessages.keySet()) {
                Long recordedTime = processedMessages.get(key);
                if (recordedTime != null && recordedTime < beforeTimestamp) {
                    if (processedMessages.remove(key) != null) {
                        removed++;
                    }
                }
            }
            return removed;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Builds composite key from station ID and sequence number.
     * Format: "stationId:sequenceNumber" (e.g., "5:42")
     *
     * @param stationId Station ID
     * @param sequenceNumber Sequence number
     * @return Composite key string
     */
    private String buildKey(long stationId, long sequenceNumber) {
        return stationId + ":" + sequenceNumber;
    }

    /**
     * Evicts oldest entries when cache exceeds maximum size.
     * Removes roughly 10% of oldest entries to make room for new ones.
     */
    private void evictOldestEntries() {
        lock.writeLock().lock();
        try {
            if (processedMessages.size() > maxCacheSize) {
                // Find and remove the 10% oldest entries
                int toRemove = maxCacheSize / 10;
                processedMessages.entrySet().stream()
                        .sorted((a, b) -> Long.compare(a.getValue(), b.getValue()))
                        .limit(toRemove)
                        .forEach(entry -> processedMessages.remove(entry.getKey()));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
