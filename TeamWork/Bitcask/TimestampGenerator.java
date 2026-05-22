package com.example.centralstation.bitcask;

/**
 * Generates sortable, collision-resistant timestamps for filenames.
 * Format: yyyymmddhhmmss[counter]
 * Examples:
 *   - 20260520143542.bin        (2026-05-20 14:35:42)
 *   - 20260520143542_001.bin    (collision within same second)
 *
 * This ensures:
 *   1. Sorting by filename = chronological order
 *   2. No collisions even with high-frequency operations in same second
 *   3. Human-readable date+time format
 */
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimestampGenerator {

    private static String lastTimestamp = "";
    private static int counter = 0;
    private static final Object lock = new Object();
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");

    /**
     * Generate a unique, sortable timestamp string for filenames.
     * If called multiple times in the same second, appends _001, _002, etc.
     * Returns: "yyyymmddhhmmss" or "yyyymmddhhmmss_NNN"
     */
    public static String generateTimestamp() {
        synchronized (lock) {
            String current = DATE_FORMAT.format(new Date());
            
            // If this is a new second, reset counter
            if (!current.equals(lastTimestamp)) {
                lastTimestamp = current;
                counter = 0;
                return current;
            }
            
            // If called within the same second, append counter suffix
            counter++;
            return current + "_" + String.format("%03d", counter);
        }
    }

    /**
     * Convert filename timestamp back to human-readable format (for debugging/logging).
     * Example input: "20260520143542"
     * Example output: "2026-05-20 14:35:42"
     */
    public static String formatTimestamp(String filenameTimestamp) {
        try {
            // Remove counter suffix if present (e.g., "_001")
            String baseTimestamp = filenameTimestamp.split("_")[0];
            
            Date date = DATE_FORMAT.parse(baseTimestamp);
            SimpleDateFormat readableFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return readableFormat.format(date);
        } catch (Exception e) {
            return filenameTimestamp; // fallback: return as-is
        }
    }
}
