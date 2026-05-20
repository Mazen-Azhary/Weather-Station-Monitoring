import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * BitcaskClient — CLI client for Bitcask storage engine
 *
 * Commands:
 *   --view-all              Export all keys/values to CSV with timestamp
 *   --view --key=SOME_KEY   Print value of specific key to stdout
 *   --concurrent --clients=N  Start N threads querying all keys, output CSVs per thread
 */
public class BitcaskClient {

    private static final Bitcask db = new Bitcask();
    private static final String TS_FORMAT = "yyyyMMddHHmmss";

    // SimpleDateFormat for timestamp generation
    private static String getCurrentTimestamp() {
        return new java.text.SimpleDateFormat(TS_FORMAT).format(new Date());
    }

    // -------------------------------------------------------------------------
    // Entry Point
    // -------------------------------------------------------------------------
    public static void main(String[] args) {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        String command = args[0];

        try {
            switch (command) {
                case "--view-all":
                    viewAll();
                    break;
                case "--view":
                    if (args.length < 2 || !args[1].startsWith("--key=")) {
                        System.err.println("Usage: --view --key=SOME_KEY");
                        System.exit(1);
                    }
                    String key = args[1].substring(6); // extract after "--key="
                    viewKey(key);
                    break;
                case "--concurrent":
                    int numClients = 100; // default
                    for (int i = 1; i < args.length; i++) {
                        if (args[i].startsWith("--clients=")) {
                            numClients = Integer.parseInt(args[i].substring(10));
                        }
                    }
                    concurrent(numClients);
                    break;
                default:
                    System.err.println("Unknown command: " + command);
                    printUsage();
                    System.exit(1);
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    // -------------------------------------------------------------------------
    // Command: --view-all
    // Export all keys and their latest values to a CSV file
    // File name format: yyyyMMddHHmmss.csv
    // -------------------------------------------------------------------------
    private static void viewAll() throws IOException {
        String timestamp = getCurrentTimestamp();
        String filename = timestamp + ".csv";

        // Get all cached entries (this is our authoritative source after startup)
        HashMapManager hmm = HashMapManager.getInstance();
        Map<String, String> allData = hmm.getAll();

        // Write to CSV
        try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
            writer.println("key,value");
            for (Map.Entry<String, String> entry : allData.entrySet()) {
                writer.printf("\"%s\",\"%s\"%n", escape(entry.getKey()), escape(entry.getValue()));
            }
        }

        System.out.println("✓ Exported " + allData.size() + " entries to: " + filename);
    }

    // -------------------------------------------------------------------------
    // Command: --view --key=SOME_KEY
    // Print the value of a specific key to stdout
    // -------------------------------------------------------------------------
    private static void viewKey(String key) {
        String value = db.read(key);
        if (value != null) {
            System.out.println(value);
        } else {
            System.out.println("(not found)");
            System.exit(1); // Exit with error if key not found
        }
    }

    // -------------------------------------------------------------------------
    // Command: --concurrent --clients=N
    // Spawn N threads, each queries all keys and outputs to CSV
    // File names: yyyyMMddHHmmss_thread_1.csv, yyyyMMddHHmmss_thread_2.csv, etc.
    // -------------------------------------------------------------------------
    private static void concurrent(int numClients) throws IOException, InterruptedException {
        String timestamp = getCurrentTimestamp();
        ExecutorService executor = Executors.newFixedThreadPool(numClients);
        CountDownLatch latch = new CountDownLatch(numClients);

        // Get all keys once (all threads will query the same keys)
        HashMapManager hmm = HashMapManager.getInstance();
        Map<String, String> allData = hmm.getAll();
        List<String> allKeys = new ArrayList<>(allData.keySet());

        System.out.println("Starting " + numClients + " concurrent client threads...");
        long startTime = System.currentTimeMillis();

        for (int i = 1; i <= numClients; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    queryAndExport(threadId, timestamp, allKeys);
                } catch (IOException e) {
                    System.err.println("[Thread " + threadId + "] Error: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        // Wait for all threads to complete
        latch.await();
        executor.shutdown();

        long elapsedMs = System.currentTimeMillis() - startTime;
        System.out.println("✓ Completed " + numClients + " threads in " + elapsedMs + " ms");
    }

    // -------------------------------------------------------------------------
    // Worker: Query all keys and export to thread-specific CSV
    // -------------------------------------------------------------------------
    private static void queryAndExport(int threadId, String timestamp, List<String> keys) throws IOException {
        String filename = String.format("%s_thread_%d.csv", timestamp, threadId);
        int successCount = 0;
        int missCount = 0;

        try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
            writer.println("key,value");

            for (String key : keys) {
                String value = db.read(key);
                if (value != null) {
                    writer.printf("\"%s\",\"%s\"%n", escape(key), escape(value));
                    successCount++;
                } else {
                    missCount++;
                }
            }
        }

        System.out.println("[Thread " + threadId + "] Exported " + successCount + " hits, "
                + missCount + " misses → " + filename);
    }

    // -------------------------------------------------------------------------
    // CSV Escaping: Handle commas, quotes, newlines
    // -------------------------------------------------------------------------
    private static String escape(String value) {
        if (value == null) return "";
        // If contains comma, quote, or newline, wrap in quotes and escape inner quotes
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return value.replace("\"", "\"\"");
        }
        return value;
    }

    // -------------------------------------------------------------------------
    // Usage
    // -------------------------------------------------------------------------
    private static void printUsage() {
        System.err.println();
        System.err.println("BitcaskClient — CLI for Bitcask storage engine");
        System.err.println();
        System.err.println("Usage:");
        System.err.println("  java BitcaskClient --view-all");
        System.err.println("    → Export all keys/values to CSV (yyyyMMddHHmmss.csv)");
        System.err.println();
        System.err.println("  java BitcaskClient --view --key=SOME_KEY");
        System.err.println("    → Print value of SOME_KEY to stdout");
        System.err.println();
        System.err.println("  java BitcaskClient --concurrent --clients=N");
        System.err.println("    → Spawn N threads, each exports to yyyyMMddHHmmss_thread_N.csv");
        System.err.println();
    }
}
