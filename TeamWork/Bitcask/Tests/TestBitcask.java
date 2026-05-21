import java.io.*;
import java.time.*;
import java.time.format.*;
import java.util.*;

// =============================================================================
//  BitcaskCLI — interactive terminal dashboard for the Bitcask storage engine
//
//  Commands
//  ────────
//  w <id> <value>   write a record
//  r <id>           read a record
//  map              view HashMapManager cache
//  files            view FileManager file list
//  hint             view global.hint file contents
//  active           view active data file contents (hex + parsed)
//  metrics          show performance metrics
//  trace            view full event trace
//  clear            clear trace log + reset metrics
//  help             show this menu
//  exit             quit
// =============================================================================
public class TestBitcask {

    // ── ANSI colours ──────────────────────────────────────────────────────────
    private static final String RESET   = "\u001B[0m";
    private static final String BOLD    = "\u001B[1m";
    private static final String DIM     = "\u001B[2m";
    private static final String RED     = "\u001B[31m";
    private static final String GREEN   = "\u001B[32m";
    private static final String YELLOW  = "\u001B[33m";
    private static final String CYAN    = "\u001B[36m";
    private static final String WHITE   = "\u001B[97m";
    private static final String BLUE    = "\u001B[34m";
    private static final String MAGENTA = "\u001B[35m";
    private static final String ORANGE  = "\u001B[38;5;208m";

    // ── Trace log ─────────────────────────────────────────────────────────────
    private static final List<String> traceLog = new ArrayList<>();
    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private static void log(String level, String colour, String msg) {
        String line = DIM + "[" + LocalTime.now().format(TS_FMT) + "]" + RESET
                + " " + colour + BOLD + "[" + level + "]" + RESET
                + " " + msg;
        traceLog.add(line);
        System.out.println(line);
    }

    private static void logInfo (String msg) { log("INFO ",  CYAN,    msg); }
    private static void logHit  (String msg) { log("HIT  ",  GREEN,   msg); }
    private static void logMiss (String msg) { log("MISS ",  YELLOW,  msg); }
    private static void logWrite(String msg) { log("WRITE",  BLUE,    msg); }
    private static void logEvent(String msg) { log("EVENT",  MAGENTA, msg); }
    private static void logError(String msg) { log("ERROR",  RED,     msg); }
    private static void logPerf (String msg) { log("PERF ",  ORANGE,  msg); }

    // ── Performance metrics ───────────────────────────────────────────────────
    private static long   totalWrites       = 0;
    private static long   totalReads        = 0;
    private static long   cacheHits         = 0;
    private static long   cacheMisses       = 0;
    private static long   totalWriteNanos   = 0;
    private static long   totalReadNanos    = 0;
    private static long   minWriteNanos     = Long.MAX_VALUE;
    private static long   maxWriteNanos     = 0;
    private static long   minReadNanos      = Long.MAX_VALUE;
    private static long   maxReadNanos      = 0;

    private static void recordWrite(long nanos) {
        totalWrites++;
        totalWriteNanos += nanos;
        if (nanos < minWriteNanos) minWriteNanos = nanos;
        if (nanos > maxWriteNanos) maxWriteNanos = nanos;
    }

    private static void recordRead(long nanos, boolean hit) {
        totalReads++;
        totalReadNanos += nanos;
        if (nanos < minReadNanos) minReadNanos = nanos;
        if (nanos > maxReadNanos) maxReadNanos = nanos;
        if (hit) cacheHits++; else cacheMisses++;
    }

    private static void resetMetrics() {
        totalWrites = totalReads = cacheHits = cacheMisses = 0;
        totalWriteNanos = totalReadNanos = 0;
        minWriteNanos = minReadNanos = Long.MAX_VALUE;
        maxWriteNanos = maxReadNanos = 0;
    }

    // ── TracedBitcask ─────────────────────────────────────────────────────────
    static class TracedBitcask {

        private final Bitcask db = new Bitcask();

        public void write(String id, String value) {
            logWrite("WRITE  id=" + BOLD + id + RESET + "  value=" + BOLD + value + RESET);
            logEvent("  → WriteManager.write(\"" + id + "\", \"" + value + "\")");
            logEvent("  → FileManager.writeRecord() — appending to active file");

            long t0 = System.nanoTime();
            db.write(id, value);
            long elapsed = System.nanoTime() - t0;

            logEvent("  → HintFileManager.write() — hint file updated on disk");
            logEvent("  → HashMapManager.write()  — cache warmed");
            logPerf ("  ⏱  write latency: " + BOLD + fmtNanos(elapsed) + RESET);
            logHit  ("  ✓ Write complete");

            recordWrite(elapsed);
        }

        public String read(String id) {
            logInfo("READ   id=" + BOLD + id + RESET);

            HashMapManager hmm = HashMapManager.getInstance();
            String cached = hmm.read(id);
            boolean hit   = cached != null;

            if (hit) {
                logHit("  → HashMapManager cache HIT  value=" + BOLD + cached + RESET);
                logEvent("  → No disk access required");
            } else {
                logMiss("  → HashMapManager cache MISS");
                logEvent("  → HintFileManager: looking up hint index ...");
            }

            long t0 = System.nanoTime();
            String result = db.read(id);
            long elapsed  = System.nanoTime() - t0;

            if (result == null) {
                logError("  ✗ id=\"" + id + "\" not found");
            } else {
                if (!hit) {
                    logEvent("  → hint entry found — direct seek into data file");
                    logEvent("  → HashMapManager.write() — cache warmed after miss");
                }
                logHit("  ✓ Returned: " + BOLD + result + RESET);
            }
            logPerf("  ⏱  read latency:  " + BOLD + fmtNanos(elapsed) + RESET
                    + DIM + (hit ? "  (cache)" : "  (disk)") + RESET);

            recordRead(elapsed, hit);
            return result;
        }
    }

    // ── View: hint file ───────────────────────────────────────────────────────
    private static void viewHintFile() {
        File hint = new File("../Files/global.hint");
        System.out.println();
        System.out.println(CYAN + BOLD + "  ┌── global.hint (" + fmtBytes(hint.length()) + ") ──────────────────────────┐" + RESET);

        if (!hint.exists() || hint.length() == 0) {
            System.out.println(DIM + "  │  (empty or not yet created)" + RESET);
            System.out.println(CYAN + BOLD + "  └──────────────────────────────────────────────────┘" + RESET);
            System.out.println();
            return;
        }

        // Binary format per entry: [4B idLen][id][4B fnLen][filename][8B offset][4B size]
        // We scan the whole file; last entry per id = latest (append-only log).
        // Display deduplicated (latest) entries only.
        Map<String, String[]> dedupMap = new LinkedHashMap<>();

        try (RandomAccessFile raf = new RandomAccessFile(hint, "r")) {
            int entryNum = 0;
            while (raf.getFilePointer() < raf.length()) {
                long entryOffset = raf.getFilePointer();

                int    idLen   = raf.readInt();
                byte[] idBytes = new byte[idLen];
                raf.readFully(idBytes);

                int    fnLen   = raf.readInt();
                byte[] fnBytes = new byte[fnLen];
                raf.readFully(fnBytes);

                long offset = raf.readLong();
                int  size   = raf.readInt();

                String id       = new String(idBytes);
                String filename = new String(fnBytes);

                // store latest: [filename, offset, size, entryOffset]
                dedupMap.put(id, new String[]{
                        filename,
                        String.valueOf(offset),
                        String.valueOf(size),
                        String.valueOf(entryOffset)
                });
                entryNum++;
            }

            System.out.printf(DIM + "  │  %d raw entries → %d unique ids%n" + RESET,
                    /* raw count */ countRawHintEntries(hint), dedupMap.size());
            System.out.println(DIM + "  │" + RESET);
            System.out.printf("  │  " + WHITE + BOLD + "%-20s  %-30s  %10s  %6s%n" + RESET,
                    "ID", "FILE", "OFFSET", "SIZE");
            System.out.println(DIM + "  │  " + "─".repeat(72) + RESET);

            for (Map.Entry<String, String[]> e : dedupMap.entrySet()) {
                String[] v = e.getValue();
                System.out.printf("  │  " + CYAN + "%-20s" + RESET
                                + "  " + WHITE + "%-30s" + RESET
                                + "  " + YELLOW + "%10s" + RESET
                                + "  " + GREEN  + "%6s" + RESET + "%n",
                        e.getKey(), v[0], v[1], v[2]);
            }

        } catch (IOException ex) {
            logError("Failed to read hint file: " + ex.getMessage());
        }

        System.out.println(CYAN + BOLD + "  └──────────────────────────────────────────────────┘" + RESET);
        System.out.println();
    }

    private static int countRawHintEntries(File hint) {
        int count = 0;
        try (RandomAccessFile raf = new RandomAccessFile(hint, "r")) {
            while (raf.getFilePointer() < raf.length()) {
                int idLen = raf.readInt();
                raf.skipBytes(idLen);
                int fnLen = raf.readInt();
                raf.skipBytes(fnLen);
                raf.skipBytes(8 + 4); // offset + size
                count++;
            }
        } catch (IOException ignored) {}
        return count;
    }

    // ── View: active file ─────────────────────────────────────────────────────
    private static void viewActiveFile() {
        FileManager fm     = FileManager.getInstance();
        File        active = fm.getActiveFile();

        System.out.println();
        if (active == null || !active.exists()) {
            System.out.println(RED + "  No active file." + RESET);
            return;
        }

        System.out.println(CYAN + BOLD + "  ┌── Active file: " + active.getName()
                + "  (" + fmtBytes(active.length()) + ") ──────────┐" + RESET);

        if (active.length() == 0) {
            System.out.println(DIM + "  │  (empty)" + RESET);
            System.out.println(CYAN + BOLD + "  └──────────────────────────────────────────────────┘" + RESET);
            System.out.println();
            return;
        }

        // Parse binary records: [4B idLen][id][4B valLen][val]
        try (RandomAccessFile raf = new RandomAccessFile(active, "r")) {
            int    recNum = 0;
            long   pos    = 0;

            System.out.printf("  │  " + WHITE + BOLD + "%-5s  %-10s  %-20s  %-20s  %s%n" + RESET,
                    "#", "OFFSET", "ID", "VALUE", "SIZE");
            System.out.println(DIM + "  │  " + "─".repeat(72) + RESET);

            while (raf.getFilePointer() < raf.length()) {
                pos = raf.getFilePointer();

                int    idLen   = raf.readInt();
                byte[] idBytes = new byte[idLen];
                raf.readFully(idBytes);

                int    valLen   = raf.readInt();
                byte[] valBytes = new byte[valLen];
                raf.readFully(valBytes);

                int totalSize = 4 + idLen + 4 + valLen;

                System.out.printf("  │  " + DIM + "%-5d" + RESET
                                + "  " + YELLOW + "%-10d" + RESET
                                + "  " + CYAN   + "%-20s" + RESET
                                + "  " + GREEN  + "%-20s" + RESET
                                + "  " + DIM    + "%d B" + RESET + "%n",
                        recNum++, pos,
                        new String(idBytes),
                        new String(valBytes),
                        totalSize);
            }

            System.out.println(DIM + "  │" + RESET);
            System.out.println(DIM + "  │  " + recNum + " records  •  "
                    + fmtBytes(active.length()) + " on disk" + RESET);

        } catch (IOException ex) {
            logError("Failed to read active file: " + ex.getMessage());
        }

        System.out.println(CYAN + BOLD + "  └──────────────────────────────────────────────────┘" + RESET);
        System.out.println();
    }

    // ── View: metrics ─────────────────────────────────────────────────────────
    private static void viewMetrics() {
        double hitRate = (totalReads == 0) ? 0.0
                : (cacheHits * 100.0 / totalReads);
        double avgWrite = (totalWrites == 0) ? 0.0
                : (totalWriteNanos / (double) totalWrites);
        double avgRead  = (totalReads  == 0) ? 0.0
                : (totalReadNanos  / (double) totalReads);

        System.out.println();
        System.out.println(CYAN + BOLD + "  ┌── Performance Metrics ──────────────────────────────┐" + RESET);
        System.out.println(DIM  + "  │" + RESET);

        // Writes
        System.out.println(BLUE + BOLD + "  │  WRITES" + RESET);
        System.out.printf ("  │    Total:          " + WHITE + "%d%n" + RESET, totalWrites);
        System.out.printf ("  │    Avg latency:    " + WHITE + "%s%n" + RESET, fmtNanos((long)avgWrite));
        System.out.printf ("  │    Min latency:    " + GREEN  + "%s%n" + RESET,
                totalWrites == 0 ? "—" : fmtNanos(minWriteNanos));
        System.out.printf ("  │    Max latency:    " + RED    + "%s%n" + RESET,
                totalWrites == 0 ? "—" : fmtNanos(maxWriteNanos));
        System.out.println(DIM  + "  │" + RESET);

        // Reads
        System.out.println(MAGENTA + BOLD + "  │  READS" + RESET);
        System.out.printf ("  │    Total:          " + WHITE + "%d%n" + RESET, totalReads);
        System.out.printf ("  │    Avg latency:    " + WHITE + "%s%n" + RESET, fmtNanos((long)avgRead));
        System.out.printf ("  │    Min latency:    " + GREEN  + "%s%n" + RESET,
                totalReads == 0 ? "—" : fmtNanos(minReadNanos));
        System.out.printf ("  │    Max latency:    " + RED    + "%s%n" + RESET,
                totalReads == 0 ? "—" : fmtNanos(maxReadNanos));
        System.out.println(DIM  + "  │" + RESET);

        // Cache
        System.out.println(CYAN + BOLD + "  │  CACHE" + RESET);
        System.out.printf ("  │    Hits:           " + GREEN  + "%d%n" + RESET, cacheHits);
        System.out.printf ("  │    Misses:         " + YELLOW + "%d%n" + RESET, cacheMisses);
        System.out.printf ("  │    Hit rate:       ");
        String rateColour = hitRate >= 80 ? GREEN : (hitRate >= 50 ? YELLOW : RED);
        System.out.printf (rateColour + BOLD + "%.1f%%%n" + RESET, hitRate);

        // File stats
        System.out.println(DIM  + "  │" + RESET);
        System.out.println(ORANGE + BOLD + "  │  FILES" + RESET);
        FileManager fm = FileManager.getInstance();
        List<File> files = fm.getFiles();
        long totalBytes = files.stream().mapToLong(File::length).sum();
        System.out.printf ("  │    Data files:     " + WHITE + "%d%n" + RESET, files.size());
        System.out.printf ("  │    Total size:     " + WHITE + "%s%n" + RESET, fmtBytes(totalBytes));
        File active = fm.getActiveFile();
        if (active != null) {
            double fillPct = active.length() * 100.0 / fm.getMaxFileSizeBytes();
            String fillColour = fillPct >= 90 ? RED : (fillPct >= 60 ? YELLOW : GREEN);
            System.out.printf("  │    Active fill:    " + fillColour + "%.1f%%%n" + RESET, fillPct);
        }

        System.out.println(CYAN + BOLD + "  └──────────────────────────────────────────────────┘" + RESET);
        System.out.println();
    }

    // ── CSV Export & Benchmark ────────────────────────────────────────────────
    private static void exportAllToCSV() {
        try {
            String timestamp = new java.text.SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
            String filename = timestamp + ".csv";

            HashMapManager hmm = HashMapManager.getInstance();
            Map<String, String> all = hmm.getAll();

            try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
                writer.println("key,value");
                for (Map.Entry<String, String> e : all.entrySet()) {
                    writer.printf("\"%s\",\"%s\"%n", csvEscape(e.getKey()), csvEscape(e.getValue()));
                }
            }

            logPerf("Exported " + all.size() + " entries to: " + filename);
        } catch (IOException e) {
            logError("Export failed: " + e.getMessage());
        }
    }

    private static void queryKey(String key) {
        Bitcask db = new Bitcask();
        String value = db.read(key);
        if (value != null) {
            System.out.println(GREEN + BOLD + "  ✓ " + key + " = " + RESET + value);
            logHit("Retrieved: " + key);
        } else {
            System.out.println(RED + "  ✗ Key not found: " + key + RESET);
            logMiss("Key not found: " + key);
        }
    }

    private static void benchmarkConcurrent(int numClients) {
        try {
            String timestamp = new java.text.SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
            HashMapManager hmm = HashMapManager.getInstance();
            Map<String, String> all = hmm.getAll();
            List<String> keys = new ArrayList<>(all.keySet());

            logEvent("Starting " + numClients + " concurrent benchmark threads...");
            long startTime = System.currentTimeMillis();

            java.util.concurrent.ExecutorService executor = 
                java.util.concurrent.Executors.newFixedThreadPool(numClients);
            java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(numClients);

            for (int i = 1; i <= numClients; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        benchmarkThread(threadId, timestamp, keys);
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();
            executor.shutdown();

            long elapsedMs = System.currentTimeMillis() - startTime;
            logPerf("Benchmark completed: " + numClients + " threads in " + elapsedMs + " ms");
        } catch (Exception e) {
            logError("Benchmark failed: " + e.getMessage());
        }
    }

    private static void benchmarkThread(int threadId, String timestamp, List<String> keys) {
        try {
            String filename = String.format("%s_thread_%d.csv", timestamp, threadId);
            int successCount = 0;
            Bitcask db = new Bitcask();

            try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
                writer.println("key,value");
                for (String key : keys) {
                    String value = db.read(key);
                    if (value != null) {
                        writer.printf("\"%s\",\"%s\"%n", csvEscape(key), csvEscape(value));
                        successCount++;
                    }
                }
            }

            logInfo("[Thread " + threadId + "] Exported " + successCount + "/" + keys.size() 
                    + " entries → " + filename);
        } catch (IOException e) {
            logError("[Thread " + threadId + "] Failed: " + e.getMessage());
        }
    }

    private static String csvEscape(String value) {
        if (value == null) return "";
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return value.replace("\"", "\"\"");
        }
        return value;
    }

    // ── Existing helpers ──────────────────────────────────────────────────────
    private static void printBanner() {
        System.out.println(CYAN + BOLD);
        System.out.println("  ██████╗ ██╗████████╗ ██████╗ █████╗ ███████╗██╗  ██╗");
        System.out.println("  ██╔══██╗██║╚══██╔══╝██╔════╝██╔══██╗██╔════╝██║ ██╔╝");
        System.out.println("  ██████╔╝██║   ██║   ██║     ███████║███████╗█████╔╝ ");
        System.out.println("  ██╔══██╗██║   ██║   ██║     ██╔══██║╚════██║██╔═██╗ ");
        System.out.println("  ██████╔╝██║   ██║   ╚██████╗██║  ██║███████║██║  ██╗");
        System.out.println("  ╚═════╝ ╚═╝   ╚═╝    ╚═════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝");
        System.out.println(RESET + DIM + "  Log-structured key-value storage engine — debug console" + RESET);
        System.out.println();
    }

    private static void printMenu() {
        System.out.println(WHITE + BOLD + "┌─── MENU ──────────────────────────────────────────────┐" + RESET);
        System.out.println(WHITE + "│  " + CYAN   + "w <id> <value>" + WHITE + "   write a record                      │");
        System.out.println("│  " + CYAN   + "r <id>"         + WHITE + "           read a record                       │");
        System.out.println("│  " + CYAN   + "map"            + WHITE + "             view HashMapManager cache          │");
        System.out.println("│  " + CYAN   + "files"          + WHITE + "           view FileManager file list           │");
        System.out.println("│  " + YELLOW + "hint"           + WHITE + "            view global.hint file               │");
        System.out.println("│  " + YELLOW + "active"         + WHITE + "          view active data file (parsed)        │");
        System.out.println("│  " + ORANGE + "metrics"        + WHITE + "         view performance metrics               │");
        System.out.println("│  " + MAGENTA+ "trace"          + WHITE + "           view full event trace                │");
        System.out.println("│  " + GREEN  + "export"         + WHITE + "          export all keys to CSV                 │");
        System.out.println("│  " + GREEN  + "query <key>"    + WHITE + "       query specific key (like BitcaskClient)   │");
        System.out.println("│  " + GREEN  + "bench <n>"      + WHITE + "       concurrent benchmark with n threads      │");
        System.out.println("│  " + MAGENTA+ "clear"          + WHITE + "           clear trace + reset metrics          │");
        System.out.println("│  " + CYAN   + "help"           + WHITE + "            show this menu                      │");
        System.out.println("│  " + RED    + "exit"           + WHITE + "            quit                                │");
        System.out.println("└────────────────────────────────────────────────────────┘" + RESET);
    }

    private static void viewHashMap() {
        HashMapManager hmm = HashMapManager.getInstance();
        Map<String, String> all = hmm.getAll();
        System.out.println();
        System.out.println(CYAN + BOLD + "  ┌── HashMapManager (in-memory cache) ─────────────┐" + RESET);
        if (all.isEmpty()) {
            System.out.println(DIM + "  │  (empty)" + RESET);
        } else {
            int i = 0;
            for (Map.Entry<String, String> e : all.entrySet()) {
                System.out.printf(WHITE + "  │  [%3d]  " + BOLD + "%-20s" + RESET + WHITE + " → " + GREEN + "%s" + RESET + "%n",
                        i++, e.getKey(), e.getValue());
            }
            System.out.println(DIM + "  │  " + all.size() + " entries" + RESET);
        }
        System.out.println(CYAN + BOLD + "  └──────────────────────────────────────────────────┘" + RESET);
        System.out.println();
    }

    private static void viewFiles() {
        FileManager fm = FileManager.getInstance();
        List<File> files = fm.getFiles();
        File active = fm.getActiveFile();
        System.out.println();
        System.out.println(CYAN + BOLD + "  ┌── FileManager — known data files ───────────────┐" + RESET);
        if (files.isEmpty()) {
            System.out.println(DIM + "  │  (no files)" + RESET);
        } else {
            for (File f : files) {
                boolean isActive = f.equals(active);
                String marker = isActive ? GREEN + BOLD + " ◄ ACTIVE" + RESET : "";
                System.out.printf("  │  " + WHITE + "%-40s" + DIM + " %8s" + RESET + "%s%n",
                        f.getName(), fmtBytes(f.length()), marker);
            }
            System.out.println(DIM + "  │  " + files.size() + " file(s)" + RESET);
        }
        System.out.println(CYAN + BOLD + "  └──────────────────────────────────────────────────┘" + RESET);
        System.out.println();
    }

    private static void viewTrace() {
        System.out.println();
        System.out.println(CYAN + BOLD + "  ┌── Event Trace (" + traceLog.size() + " entries) ──────────────────────┐" + RESET);
        if (traceLog.isEmpty()) {
            System.out.println(DIM + "  │  (no events yet)" + RESET);
        } else {
            for (String line : traceLog) {
                System.out.println("  │ " + line);
            }
        }
        System.out.println(CYAN + BOLD + "  └──────────────────────────────────────────────────┘" + RESET);
        System.out.println();
    }

    private static void printSep() {
        System.out.println(DIM + "  ─────────────────────────────────────────────────────" + RESET);
    }

    // ── Formatting utils ──────────────────────────────────────────────────────
    private static String fmtNanos(long nanos) {
        if (nanos < 1_000)           return nanos + " ns";
        if (nanos < 1_000_000)       return String.format("%.2f µs", nanos / 1_000.0);
        if (nanos < 1_000_000_000)   return String.format("%.2f ms", nanos / 1_000_000.0);
        return                              String.format("%.3f s",  nanos / 1_000_000_000.0);
    }

    private static String fmtBytes(long bytes) {
        if (bytes < 1024)            return bytes + " B";
        if (bytes < 1024 * 1024)     return String.format("%.2f KB", bytes / 1024.0);
        return                              String.format("%.2f MB", bytes / (1024.0 * 1024));
    }

    // ── Entry point ───────────────────────────────────────────────────────────
    public static void main(String[] args) {
        printBanner();
        logInfo("Bitcask engine initialising ...");
        TracedBitcask db = new TracedBitcask();
        logInfo("Engine ready. Files directory: ./Files/");
        System.out.println();
        printMenu();
        System.out.println();

        Scanner sc = new Scanner(System.in);

        while (true) {
            System.out.print(BOLD + CYAN + "bitcask> " + RESET);
            String line = sc.nextLine().trim();
            if (line.isEmpty()) continue;

            String[] parts = line.split("\\s+", 3);
            String cmd = parts[0].toLowerCase();

            printSep();

            switch (cmd) {

                case "w": case "write":
                    if (parts.length < 3) { logError("Usage: w <id> <value>"); }
                    else                  { db.write(parts[1], parts[2]); }
                    break;

                case "r": case "read":
                    if (parts.length < 2) { logError("Usage: r <id>"); }
                    else {
                        String val = db.read(parts[1]);
                        if (val != null) System.out.println(GREEN + BOLD + "  Result: " + RESET + val);
                        else             System.out.println(RED + "  Not found." + RESET);
                    }
                    break;

                case "map":     viewHashMap();   break;
                case "files":   viewFiles();     break;
                case "hint":    viewHintFile();  break;
                case "active":  viewActiveFile();break;
                case "metrics": viewMetrics();   break;
                case "trace":   viewTrace();     break;

                case "export":
                    exportAllToCSV();
                    break;

                case "query":
                    if (parts.length < 2) { logError("Usage: query <key>"); }
                    else                  { queryKey(parts[1]); }
                    break;

                case "bench":
                    int numClients = 100;
                    if (parts.length >= 2) {
                        try { numClients = Integer.parseInt(parts[1]); }
                        catch (NumberFormatException e) { logError("Invalid client count"); break; }
                    }
                    benchmarkConcurrent(numClients);
                    break;

                case "clear":
                    traceLog.clear();
                    resetMetrics();
                    logInfo("Trace log cleared and metrics reset.");
                    break;

                case "help":    printMenu();     break;

                case "exit": case "quit":
                    logInfo("Shutting down. Goodbye.");
                    System.exit(0);
                    break;

                default:
                    logError("Unknown command: \"" + cmd + "\" — type 'help' for options");
            }

            printSep();
            System.out.println();
        }
    }
}