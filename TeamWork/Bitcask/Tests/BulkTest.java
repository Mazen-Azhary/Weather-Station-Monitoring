import java.util.*;

// =============================================================================
//  BulkTest — loads 10,000 synthetic records into Bitcask and reports results
//
//  Run once:  javac *.java && java BulkTest
//
//  Phases
//  ──────
//  1. BULK WRITE  — 10,000 records with random IDs + statuses
//  2. FULL READ   — read every written record back (exercises cache + hint)
//  3. UPDATE PASS — overwrite ~20% of records with new statuses
//  4. MISS TEST   — read 200 IDs that were never written (pure miss path)
//  5. SUMMARY     — latency table, cache stats, file inventory
// =============================================================================
public class BulkTest {

    // ── ANSI ──────────────────────────────────────────────────────────────────
    private static final String RESET   = "\u001B[0m";
    private static final String BOLD    = "\u001B[1m";
    private static final String DIM     = "\u001B[2m";
    private static final String RED     = "\u001B[31m";
    private static final String GREEN   = "\u001B[32m";
    private static final String YELLOW  = "\u001B[33m";
    private static final String CYAN    = "\u001B[36m";
    private static final String BLUE    = "\u001B[34m";
    private static final String MAGENTA = "\u001B[35m";
    private static final String WHITE   = "\u001B[97m";
    private static final String ORANGE  = "\u001B[38;5;208m";

    // ── Config ────────────────────────────────────────────────────────────────
    private static final int    TOTAL_RECORDS  = 10_000;
    private static final int    UPDATE_PCT     = 20;        // % of records to overwrite
    private static final int    MISS_PROBES    = 200;       // reads on unknown IDs
    private static final int    PROGRESS_STEP  = 500;       // print progress every N ops

    private static final String[] STATUSES = {
            "OK", "WARN", "CRITICAL", "OFFLINE", "DEGRADED",
            "NOMINAL", "ERROR", "MAINTENANCE", "RECOVERING", "UNKNOWN"
    };

    private static final String[] PREFIXES = {
            "ATL", "CDG", "LAX", "JFK", "HND", "DXB", "SYD", "GRU", "CPT", "BOM",
            "ORD", "FRA", "SIN", "ICN", "MEX", "MUC", "YYZ", "ZRH", "AMS", "MAD"
    };

    private static final Random RNG = new Random(42); // fixed seed = reproducible

    // ── Metrics state ─────────────────────────────────────────────────────────
    private static long   writeCount, readCount, updateCount, missCount;
    private static long   writeTotalNs, readTotalNs, updateTotalNs;
    private static long   writeMinNs = Long.MAX_VALUE, writeMaxNs;
    private static long   readMinNs  = Long.MAX_VALUE, readMaxNs;
    private static long   cacheHits, cacheMisses;
    private static int    verifyOk, verifyFail;

    // ── Entry point ───────────────────────────────────────────────────────────
    public static void main(String[] args) {
        printBanner();
        Bitcask db = new Bitcask();

        // ── Phase 1: bulk write ───────────────────────────────────────────────
        header("PHASE 1 — BULK WRITE  (" + TOTAL_RECORDS + " records)");
        List<String> ids      = new ArrayList<>(TOTAL_RECORDS);
        List<String> statuses = new ArrayList<>(TOTAL_RECORDS);

        long phaseStart = System.nanoTime();
        for (int i = 0; i < TOTAL_RECORDS; i++) {
            String id     = generateId();
            String status = randomStatus();
            ids.add(id);
            statuses.add(status);

            long t0 = System.nanoTime();
            db.write(id, status);
            long ns = System.nanoTime() - t0;

            writeCount++;
            writeTotalNs += ns;
            if (ns < writeMinNs) writeMinNs = ns;
            if (ns > writeMaxNs) writeMaxNs = ns;

            if ((i + 1) % PROGRESS_STEP == 0) {
                progress("  write", i + 1, TOTAL_RECORDS, phaseStart);
            }
        }
        phaseDone(phaseStart, writeCount);

        // ── Phase 2: full read (all records — first read hits cache) ──────────
        header("PHASE 2 — FULL READ  (all " + TOTAL_RECORDS + " records)");
        phaseStart = System.nanoTime();
        for (int i = 0; i < ids.size(); i++) {
            String id       = ids.get(i);
            String expected = statuses.get(i);

            // Check cache before the call to classify hit/miss
            boolean hit = HashMapManager.getInstance().read(id) != null;

            long t0 = System.nanoTime();
            String got = db.read(id);
            long ns = System.nanoTime() - t0;

            readCount++;
            readTotalNs += ns;
            if (ns < readMinNs) readMinNs = ns;
            if (ns > readMaxNs) readMaxNs = ns;

            if (hit) cacheHits++; else cacheMisses++;

            if (got != null && got.equals(expected)) verifyOk++;
            else                                      verifyFail++;

            if ((i + 1) % PROGRESS_STEP == 0) {
                progress("  read ", i + 1, ids.size(), phaseStart);
            }
        }
        phaseDone(phaseStart, readCount);

        // ── Phase 3: update 20% of records ────────────────────────────────────
        int updateN = (int)(TOTAL_RECORDS * UPDATE_PCT / 100.0);
        header("PHASE 3 — UPDATE PASS  (" + updateN + " overwrites, " + UPDATE_PCT + "% of records)");
        Collections.shuffle(ids, RNG); // randomise which ones get updated
        phaseStart = System.nanoTime();
        for (int i = 0; i < updateN; i++) {
            String id        = ids.get(i);
            String newStatus = randomStatus();
            statuses.set(ids.indexOf(id), newStatus); // keep expected map in sync

            long t0 = System.nanoTime();
            db.write(id, newStatus);
            long ns = System.nanoTime() - t0;

            updateCount++;
            updateTotalNs += ns;
            writeCount++;
            writeTotalNs += ns;
            if (ns < writeMinNs) writeMinNs = ns;
            if (ns > writeMaxNs) writeMaxNs = ns;

            if ((i + 1) % (PROGRESS_STEP / 5) == 0) {
                progress("  updt ", i + 1, updateN, phaseStart);
            }
        }
        phaseDone(phaseStart, updateCount);

        // ── Phase 4: deliberate miss test ─────────────────────────────────────
        header("PHASE 4 — MISS TEST  (" + MISS_PROBES + " unknown IDs)");
        phaseStart = System.nanoTime();
        for (int i = 0; i < MISS_PROBES; i++) {
            String ghostId = "GHOST_" + UUID.randomUUID().toString().substring(0, 8);

            boolean hit = HashMapManager.getInstance().read(ghostId) != null;
            long t0 = System.nanoTime();
            db.read(ghostId);
            long ns = System.nanoTime() - t0;

            readCount++;
            readTotalNs += ns;
            if (ns < readMinNs) readMinNs = ns;
            if (ns > readMaxNs) readMaxNs = ns;

            if (hit) cacheHits++; else { cacheMisses++; missCount++; }
        }
        phaseDone(phaseStart, MISS_PROBES);

        // ── Phase 5: summary ──────────────────────────────────────────────────
        printSummary();
    }

    // ── Summary ───────────────────────────────────────────────────────────────
    private static void printSummary() {
        System.out.println();
        System.out.println(CYAN + BOLD
                + "╔══════════════════════════════════════════════════════════╗");
        System.out.println(  "║                    BULK TEST SUMMARY                     ║");
        System.out.println(  "╚══════════════════════════════════════════════════════════╝"
                + RESET);

        // Writes
        row(BLUE, "WRITES",  "Total",         String.valueOf(writeCount));
        row(BLUE, "",        "Avg latency",   fmtNs(writeTotalNs / writeCount));
        row(BLUE, "",        "Min latency",   fmtNs(writeMinNs));
        row(BLUE, "",        "Max latency",   fmtNs(writeMaxNs));
        row(BLUE, "",        "Throughput",    String.format("%.0f ops/s",
                writeCount / (writeTotalNs / 1_000_000_000.0)));
        sep();

        // Reads
        long allReads = readCount;
        row(MAGENTA, "READS", "Total",        String.valueOf(allReads));
        row(MAGENTA, "",      "Avg latency",  fmtNs(readTotalNs / allReads));
        row(MAGENTA, "",      "Min latency",  fmtNs(readMinNs));
        row(MAGENTA, "",      "Max latency",  fmtNs(readMaxNs));
        row(MAGENTA, "",      "Throughput",   String.format("%.0f ops/s",
                allReads / (readTotalNs / 1_000_000_000.0)));
        sep();

        // Cache
        long totalCacheOps = cacheHits + cacheMisses;
        double hitRate = totalCacheOps == 0 ? 0 : cacheHits * 100.0 / totalCacheOps;
        String hitColour = hitRate >= 80 ? GREEN : (hitRate >= 50 ? YELLOW : RED);
        row(hitColour, "CACHE", "Hits",       String.valueOf(cacheHits));
        row(hitColour, "",      "Misses",     String.valueOf(cacheMisses));
        row(hitColour, "",      "Hit rate",   String.format("%.2f%%", hitRate));
        row(hitColour, "",      "Miss probes confirmed", String.valueOf(missCount));
        sep();

        // Correctness
        String corrColour = verifyFail == 0 ? GREEN : RED;
        row(corrColour, "CORRECTNESS", "Verified OK",   String.valueOf(verifyOk));
        row(corrColour, "",            "Mismatches",    String.valueOf(verifyFail));
        sep();

        // Files
        FileManager fm = FileManager.getInstance();
        List<java.io.File> files = fm.getFiles();
        long totalBytes = files.stream().mapToLong(java.io.File::length).sum();
        row(ORANGE, "FILES", "Data files",  String.valueOf(files.size()));
        row(ORANGE, "",      "Total size",  fmtBytes(totalBytes));
        row(ORANGE, "",      "Active file", fm.getActiveFile() != null
                ? fm.getActiveFile().getName() : "—");
        sep();

        System.out.println();
        if (verifyFail == 0) {
            System.out.println(GREEN + BOLD + "  ✓ All verifications passed." + RESET);
        } else {
            System.out.println(RED + BOLD + "  ✗ " + verifyFail + " verification(s) failed!" + RESET);
        }
        System.out.println();
    }

    // ── Generators ───────────────────────────────────────────────────────────
    private static String generateId() {
        String prefix = PREFIXES[RNG.nextInt(PREFIXES.length)];
        int    num    = 1000 + RNG.nextInt(9000); // 4-digit number
        return prefix + "-" + num;
    }

    private static String randomStatus() {
        return STATUSES[RNG.nextInt(STATUSES.length)];
    }

    // ── Display helpers ───────────────────────────────────────────────────────
    private static void printBanner() {
        System.out.println(CYAN + BOLD);
        System.out.println("  ██████╗ ██╗   ██╗██╗     ██╗  ██╗    ████████╗███████╗███████╗████████╗");
        System.out.println("  ██╔══██╗██║   ██║██║     ██║ ██╔╝    ╚══██╔══╝██╔════╝██╔════╝╚══██╔══╝");
        System.out.println("  ██████╔╝██║   ██║██║     █████╔╝        ██║   █████╗  ███████╗   ██║   ");
        System.out.println("  ██╔══██╗██║   ██║██║     ██╔═██╗        ██║   ██╔══╝  ╚════██║   ██║   ");
        System.out.println("  ██████╔╝╚██████╔╝███████╗██║  ██╗       ██║   ███████╗███████║   ██║   ");
        System.out.println("  ╚═════╝  ╚═════╝ ╚══════╝╚═╝  ╚═╝       ╚═╝   ╚══════╝╚══════╝   ╚═╝   ");
        System.out.println(RESET + DIM + "  Bitcask bulk test — 10,000 synthetic records" + RESET);
        System.out.println();
    }

    private static void header(String title) {
        System.out.println();
        System.out.println(CYAN + BOLD + "  ┌── " + title + " ──" + RESET);
    }

    private static void progress(String label, int done, int total, long startNs) {
        double pct     = done * 100.0 / total;
        long   elapsed = System.nanoTime() - startNs;
        double rate    = done / (elapsed / 1_000_000_000.0);
        int    bars    = (int)(pct / 5);   // 20-char bar
        String bar     = GREEN + "█".repeat(bars) + DIM + "░".repeat(20 - bars) + RESET;
        System.out.printf("  │ %s [%s] " + WHITE + "%5.1f%%  %6d/%d  " + DIM + "%.0f ops/s%n" + RESET,
                label, bar, pct, done, total, rate);
    }

    private static void phaseDone(long startNs, long count) {
        double secs = (System.nanoTime() - startNs) / 1_000_000_000.0;
        System.out.printf(GREEN + "  └── done  %d ops  in  %.3f s  (%.0f ops/s)%n" + RESET,
                count, secs, count / secs);
    }

    private static void row(String colour, String section, String key, String value) {
        String sec = section.isEmpty() ? "           " : String.format("%-12s", section);
        System.out.printf("  " + colour + BOLD + "%-12s" + RESET + WHITE + "  %-22s" + RESET
                + colour + BOLD + "%s%n" + RESET, sec, key, value);
    }

    private static void sep() {
        System.out.println(DIM + "  " + "─".repeat(52) + RESET);
    }

    private static String fmtNs(long ns) {
        if (ns < 1_000)           return ns + " ns";
        if (ns < 1_000_000)       return String.format("%.2f µs", ns / 1_000.0);
        if (ns < 1_000_000_000)   return String.format("%.2f ms", ns / 1_000_000.0);
        return                           String.format("%.3f s",  ns / 1_000_000_000.0);
    }

    private static String fmtBytes(long bytes) {
        if (bytes < 1024)         return bytes + " B";
        if (bytes < 1024 * 1024)  return String.format("%.2f KB", bytes / 1024.0);
        return                           String.format("%.2f MB", bytes / (1024.0 * 1024));
    }
}