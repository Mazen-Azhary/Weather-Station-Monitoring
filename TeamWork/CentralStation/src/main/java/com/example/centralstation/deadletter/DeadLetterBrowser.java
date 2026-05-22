package com.example.centralstation.deadletter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Command-line interface for accessing and managing Dead Letter Channel messages.
 * 
 * Usage:
 *   java DeadLetterCLI list --date 2026-05-22 [--stage PARQUET_WRITE]
 *   java DeadLetterCLI view --date 2026-05-22 --stage PARQUET_WRITE --offset 1234
 *   java DeadLetterCLI station --station-id 5
 *   java DeadLetterCLI stats --date 2026-05-22
 *   java DeadLetterCLI export --date 2026-05-22 [--stage PARQUET_WRITE] --output export.jsonl
 *   java DeadLetterCLI search --exception IOException
 *   java DeadLetterCLI replay --date 2026-05-22 --stage PARQUET_WRITE
 */
public class DeadLetterBrowser {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final Path deadLetterRoot;

    public DeadLetterBrowser(String deadLetterRoot) {
        this.deadLetterRoot = Paths.get(deadLetterRoot);
        validateRoot();
    }

    public DeadLetterBrowser() {
        this("dead-letters");
    }

    private void validateRoot() {
        if (!Files.exists(deadLetterRoot)) {
            System.err.println("✗ Dead letter directory not found: " + deadLetterRoot.toAbsolutePath());
            System.exit(1);
        }
    }

    // ============ LIST COMMAND ============

    public void listByDateStage(String dateStr, String stage) {
        Path datePath = deadLetterRoot.resolve(dateStr);

        if (!Files.exists(datePath)) {
            System.err.println("✗ No dead letters found for date: " + dateStr);
            return;
        }

        try {
            if (stage != null) {
                listStageContents(datePath, stage, dateStr);
            } else {
                listAllStages(datePath, dateStr);
            }
        } catch (IOException e) {
            System.err.println("✗ Error reading dead letters: " + e.getMessage());
        }
    }

    private void listAllStages(Path datePath, String dateStr) throws IOException {
        List<Path> stages = Files.list(datePath)
                .filter(Files::isDirectory)
                .sorted()
                .collect(Collectors.toList());

        if (stages.isEmpty()) {
            System.err.println("✗ No dead letters found for date: " + dateStr);
            return;
        }

        for (Path stageDir : stages) {
            String stageName = stageDir.getFileName().toString();
            List<Path> files = Files.list(stageDir)
                    .filter(p -> p.toString().endsWith(".json"))
                    .collect(Collectors.toList());

            System.out.println("\n[" + stageName + "] (" + files.size() + " messages)");
            files.stream().sorted().limit(5).forEach(f -> System.out.println("  - " + f.getFileName()));
            if (files.size() > 5) {
                System.out.println("  ... and " + (files.size() - 5) + " more");
            }
        }
    }

    private void listStageContents(Path datePath, String stage, String dateStr) throws IOException {
        Path stagePath = datePath.resolve(stage);

        if (!Files.exists(stagePath)) {
            System.err.println("✗ No dead letters for stage " + stage + " on " + dateStr);
            return;
        }

        List<Path> files = Files.list(stagePath)
                .filter(p -> p.toString().endsWith(".json"))
                .sorted()
                .collect(Collectors.toList());

        if (files.isEmpty()) {
            System.err.println("✗ No dead letters for stage " + stage + " on " + dateStr);
            return;
        }

        System.out.println("\n[" + stage + "] on " + dateStr + " (" + files.size() + " messages)\n");
        System.out.println(String.format("%-8s | %-20s | %-8s | %-20s", "Offset", "Timestamp", "Station", "Exception"));
        System.out.println(String.join("", java.util.Collections.nCopies(70, "-")));

        for (Path file : files) {
            try {
                DeadLetterMessage msg = mapper.readValue(Files.readAllBytes(file), DeadLetterMessage.class);
                long offset = msg.getKafkaOffset();
                Long station = msg.getStationId() != null ? msg.getStationId() : -1;
                String exception = msg.getExceptionType() != null ? msg.getExceptionType() : "Unknown";
                if (exception.length() > 20) exception = exception.substring(0, 20);

                long ts = msg.getFailedAtTimestamp();
                LocalDateTime dt = LocalDateTime.ofInstant(
                        java.time.Instant.ofEpochMilli(ts),
                        ZoneId.systemDefault()
                );
                String dtStr = dt.format(timestampFormatter);

                System.out.println(String.format("%-8d | %-20s | %-8s | %-20s", offset, dtStr, station, exception));
            } catch (Exception e) {
                System.err.println("✗ Error reading " + file.getFileName() + ": " + e.getMessage());
            }
        }
    }

    // ============ VIEW COMMAND ============

    public void viewMessage(String dateStr, String stage, long offset) {
        Path stagePath = deadLetterRoot.resolve(dateStr).resolve(stage);

        try {
            List<Path> matches = Files.list(stagePath)
                    .filter(p -> p.getFileName().toString().contains("_offset_" + offset + ".json"))
                    .collect(Collectors.toList());

            if (matches.isEmpty()) {
                System.err.println("✗ Dead letter not found: date=" + dateStr + ", stage=" + stage + ", offset=" + offset);
                return;
            }

            Path filePath = matches.get(0);
            DeadLetterMessage msg = mapper.readValue(Files.readAllBytes(filePath), DeadLetterMessage.class);

            printMessageDetails(filePath, msg);

        } catch (IOException e) {
            System.err.println("✗ Error reading dead letter: " + e.getMessage());
        }
    }

    private void printMessageDetails(Path filePath, DeadLetterMessage msg) throws IOException {
        System.out.println("\n" + String.join("", java.util.Collections.nCopies(70, "=")));
        System.out.println("Dead Letter Message: " + filePath.getFileName());
        System.out.println(String.join("", java.util.Collections.nCopies(70, "=")));
        System.out.println();

        System.out.println("Processing Stage: " + msg.getProcessingStage());
        System.out.println("Kafka Offset:     " + msg.getKafkaOffset());
        System.out.println("Kafka Partition:  " + msg.getKafkaPartition());
        System.out.println("Station ID:       " + msg.getStationId());
        System.out.println("Sequence Number:  " + msg.getSequenceNumber());

        LocalDateTime dt = LocalDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(msg.getFailedAtTimestamp()),
                ZoneId.systemDefault()
        );
        System.out.println("Failed At:        " + dt.format(timestampFormatter));

        System.out.println("\nException Type:   " + msg.getExceptionType());
        System.out.println("Exception:        " + msg.getExceptionMessage());

        System.out.println("\nFailure Reasons:");
        for (String reason : msg.getFailureReasons()) {
            System.out.println("  • " + reason);
        }

        System.out.println("\nOriginal Message (" + msg.getOriginalMessage().length() + " chars):");
        System.out.println(String.join("", java.util.Collections.nCopies(70, "-")));
        try {
            Object originalObj = mapper.readValue(msg.getOriginalMessage(), Object.class);
            System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(originalObj));
        } catch (Exception e) {
            System.out.println(msg.getOriginalMessage());
        }
        System.out.println(String.join("", java.util.Collections.nCopies(70, "-")));
    }

    // ============ STATION COMMAND ============

    public void listByStation(long stationId) {
        List<StationFailure> results = new ArrayList<>();

        try {
            Files.walk(deadLetterRoot)
                    .filter(p -> p.toString().endsWith(".json"))
                    .forEach(file -> {
                        try {
                            DeadLetterMessage msg = mapper.readValue(Files.readAllBytes(file), DeadLetterMessage.class);
                            if (msg.getStationId() != null && msg.getStationId() == stationId) {
                                String dateStr = file.getParent().getParent().getFileName().toString();
                                String stage = file.getParent().getFileName().toString();
                                results.add(new StationFailure(dateStr, stage, msg));
                            }
                        } catch (Exception ignored) {
                        }
                    });
        } catch (IOException e) {
            System.err.println("✗ Error searching dead letters: " + e.getMessage());
            return;
        }

        if (results.isEmpty()) {
            System.err.println("✗ No dead letters found for station " + stationId);
            return;
        }

        System.out.println("\n[Station " + stationId + "] (" + results.size() + " messages)\n");
        System.out.println(String.format("%-12s | %-15s | %-8s | %-20s", "Date", "Stage", "Offset", "Exception"));
        System.out.println(String.join("", java.util.Collections.nCopies(60, "-")));

        results.stream()
                .sorted(Comparator.comparing(r -> r.dateStr))
                .forEach(r -> {
                    String exc = r.msg.getExceptionType() != null ? r.msg.getExceptionType() : "Unknown";
                    if (exc.length() > 20) exc = exc.substring(0, 20);
                    System.out.println(String.format("%-12s | %-15s | %-8d | %-20s",
                            r.dateStr, r.stage, r.msg.getKafkaOffset(), exc));
                });
    }

    private static class StationFailure {
        String dateStr;
        String stage;
        DeadLetterMessage msg;

        StationFailure(String dateStr, String stage, DeadLetterMessage msg) {
            this.dateStr = dateStr;
            this.stage = stage;
            this.msg = msg;
        }
    }

    // ============ STATS COMMAND ============

    public void printStats(String dateStr) {
        Path datePath = deadLetterRoot.resolve(dateStr);

        if (!Files.exists(datePath)) {
            System.err.println("✗ No dead letters found for date: " + dateStr);
            return;
        }

        Map<String, Integer> stageCounts = new TreeMap<>();
        Map<String, Integer> exceptionCounts = new TreeMap<>();
        int total = 0;

        try {
            for (Path stageDir : Files.list(datePath).filter(Files::isDirectory).collect(Collectors.toList())) {
                String stageName = stageDir.getFileName().toString();
                int count = 0;
                
                for (Path file : Files.list(stageDir).filter(p -> p.toString().endsWith(".json")).collect(Collectors.toList())) {
                    DeadLetterMessage msg = mapper.readValue(Files.readAllBytes(file), DeadLetterMessage.class);
                    stageCounts.put(stageName, stageCounts.getOrDefault(stageName, 0) + 1);
                    String exc = msg.getExceptionType() != null ? msg.getExceptionType() : "Unknown";
                    exceptionCounts.put(exc, exceptionCounts.getOrDefault(exc, 0) + 1);
                    count++;
                    total++;
                }
            }
        } catch (IOException e) {
            System.err.println("✗ Error reading statistics: " + e.getMessage());
            return;
        }

        System.out.println("\nDead Letter Statistics: " + dateStr);
        System.out.println(String.join("", java.util.Collections.nCopies(50, "=")));
        System.out.println("Total Messages: " + total + "\n");

        System.out.println("By Processing Stage:");
        int finalTotal = total;
        stageCounts.forEach((stage, count) -> {
            double percentage = (count * 100.0) / finalTotal;
            System.out.println(String.format("  %-20s %6d (%.1f%%)", stage, count, percentage));
        });

        System.out.println("\nBy Exception Type:");
        exceptionCounts.entrySet().stream()
                .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                .forEach(e -> {
                    double percentage = (e.getValue() * 100.0) / finalTotal;
                    System.out.println(String.format("  %-25s %6d (%.1f%%)", e.getKey(), e.getValue(), percentage));
                });
    }

    // ============ EXPORT COMMAND ============

    public void exportToFile(String dateStr, String stage, String outputFile) {
        Path datePath = deadLetterRoot.resolve(dateStr);

        if (!Files.exists(datePath)) {
            System.err.println("✗ No dead letters found for date: " + dateStr);
            return;
        }

        List<String> lines = new ArrayList<>();

        try {
            for (Path stageDir : Files.list(datePath).filter(Files::isDirectory).collect(Collectors.toList())) {
                if (stage != null && !stageDir.getFileName().toString().equals(stage)) {
                    continue;
                }

                for (Path file : Files.list(stageDir).filter(p -> p.toString().endsWith(".json")).collect(Collectors.toList())) {
                    String content = new String(Files.readAllBytes(file));
                    lines.add(content);
                }
            }

            Files.write(Paths.get(outputFile), String.join("\n", lines).getBytes());
            System.out.println("✓ Exported " + lines.size() + " messages to " + outputFile);

        } catch (IOException e) {
            System.err.println("✗ Error exporting dead letters: " + e.getMessage());
        }
    }

    // ============ SEARCH COMMAND ============

    public void search(String exceptionPattern) {
        List<SearchResult> results = new ArrayList<>();
        Pattern pattern = Pattern.compile(exceptionPattern, Pattern.CASE_INSENSITIVE);

        try {
            Files.walk(deadLetterRoot)
                    .filter(p -> p.toString().endsWith(".json"))
                    .forEach(file -> {
                        try {
                            DeadLetterMessage msg = mapper.readValue(Files.readAllBytes(file), DeadLetterMessage.class);
                            if (msg.getExceptionType() != null && pattern.matcher(msg.getExceptionType()).find()) {
                                String dateStr = file.getParent().getParent().getFileName().toString();
                                String stage = file.getParent().getFileName().toString();
                                results.add(new SearchResult(dateStr, stage, msg));
                            }
                        } catch (Exception ignored) {
                        }
                    });
        } catch (IOException e) {
            System.err.println("✗ Error searching dead letters: " + e.getMessage());
            return;
        }

        if (results.isEmpty()) {
            System.err.println("✗ No dead letters found matching: " + exceptionPattern);
            return;
        }

        System.out.println("\n[Search: '" + exceptionPattern + "'] (" + results.size() + " matches)\n");
        System.out.println(String.format("%-12s | %-15s | %-8s | %-20s", "Date", "Stage", "Station", "Exception"));
        System.out.println(String.join("", java.util.Collections.nCopies(60, "-")));

        for (SearchResult r : results) {
            String exc = r.msg.getExceptionType() != null ? r.msg.getExceptionType() : "Unknown";
            if (exc.length() > 20) exc = exc.substring(0, 20);
            Long station = r.msg.getStationId() != null ? r.msg.getStationId() : -1;
            System.out.println(String.format("%-12s | %-15s | %-8s | %-20s", r.dateStr, r.stage, station, exc));
        }
    }

    private static class SearchResult {
        String dateStr;
        String stage;
        DeadLetterMessage msg;

        SearchResult(String dateStr, String stage, DeadLetterMessage msg) {
            this.dateStr = dateStr;
            this.stage = stage;
            this.msg = msg;
        }
    }

    // ============ REPLAY COMMAND ============

    public void replayInstructions(String dateStr, String stage) {
        Path stagePath = deadLetterRoot.resolve(dateStr).resolve(stage);

        try {
            List<Path> files = Files.list(stagePath)
                    .filter(p -> p.toString().endsWith(".json"))
                    .sorted()
                    .collect(Collectors.toList());

            if (files.isEmpty()) {
                System.err.println("✗ No dead letters found for " + stage + " on " + dateStr);
                return;
            }

            System.out.println("\nReplay Instructions for " + stage + " (" + files.size() + " messages)");
            System.out.println(String.join("", java.util.Collections.nCopies(60, "=")));
            System.out.println("\nTo replay these messages:\n");
            System.out.println("1. Export messages:");
            System.out.println("   java DeadLetterCLI export --date " + dateStr + " --stage " + stage + " --output replay.jsonl\n");
            System.out.println("2. Send back to Kafka topic (with error handling):");
            System.out.println("   kafka-console-producer.sh --topic weather-readings < replay.jsonl\n");
            System.out.println("3. Monitor the consumer for successful reprocessing\n");
            System.out.println("Sample files to replay:");

            files.stream().limit(3).forEach(f -> {
                try {
                    DeadLetterMessage msg = mapper.readValue(Files.readAllBytes(f), DeadLetterMessage.class);
                    System.out.println("  • " + f.getFileName() + " (offset=" + msg.getKafkaOffset() + ")");
                } catch (Exception ignored) {
                }
            });

            if (files.size() > 3) {
                System.out.println("  ... and " + (files.size() - 3) + " more");
            }

        } catch (IOException e) {
            System.err.println("✗ Error reading files: " + e.getMessage());
        }
    }

    // ============ MAIN & CLI PARSING ============

    public static void main(String[] args) {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        DeadLetterBrowser cli = new DeadLetterBrowser();
        String command = args[0];

        try {
            switch (command) {
                case "list":
                    handleListCommand(cli, args);
                    break;
                case "view":
                    handleViewCommand(cli, args);
                    break;
                case "station":
                    handleStationCommand(cli, args);
                    break;
                case "stats":
                    handleStatsCommand(cli, args);
                    break;
                case "export":
                    handleExportCommand(cli, args);
                    break;
                case "search":
                    handleSearchCommand(cli, args);
                    break;
                case "replay":
                    handleReplayCommand(cli, args);
                    break;
                default:
                    System.err.println("✗ Unknown command: " + command);
                    printUsage();
                    System.exit(1);
            }
        } catch (Exception e) {
            System.err.println("✗ Error: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static void handleListCommand(DeadLetterBrowser cli, String[] args) {
        String date = getArgument(args, "--date");
        String stage = getArgument(args, "--stage");

        if (date == null) {
            System.err.println("✗ Missing required argument: --date");
            System.exit(1);
        }

        cli.listByDateStage(date, stage);
    }

    private static void handleViewCommand(DeadLetterBrowser cli, String[] args) {
        String date = getArgument(args, "--date");
        String stage = getArgument(args, "--stage");
        String offsetStr = getArgument(args, "--offset");

        if (date == null || stage == null || offsetStr == null) {
            System.err.println("✗ Missing required arguments: --date, --stage, --offset");
            System.exit(1);
        }

        try {
            long offset = Long.parseLong(offsetStr);
            cli.viewMessage(date, stage, offset);
        } catch (NumberFormatException e) {
            System.err.println("✗ Invalid offset: " + offsetStr);
            System.exit(1);
        }
    }

    private static void handleStationCommand(DeadLetterBrowser cli, String[] args) {
        String stationIdStr = getArgument(args, "--station-id");

        if (stationIdStr == null) {
            System.err.println("✗ Missing required argument: --station-id");
            System.exit(1);
        }

        try {
            long stationId = Long.parseLong(stationIdStr);
            cli.listByStation(stationId);
        } catch (NumberFormatException e) {
            System.err.println("✗ Invalid station ID: " + stationIdStr);
            System.exit(1);
        }
    }

    private static void handleStatsCommand(DeadLetterBrowser cli, String[] args) {
        String date = getArgument(args, "--date");

        if (date == null) {
            System.err.println("✗ Missing required argument: --date");
            System.exit(1);
        }

        cli.printStats(date);
    }

    private static void handleExportCommand(DeadLetterBrowser cli, String[] args) {
        String date = getArgument(args, "--date");
        String stage = getArgument(args, "--stage");
        String output = getArgument(args, "--output");

        if (date == null || output == null) {
            System.err.println("✗ Missing required arguments: --date, --output");
            System.exit(1);
        }

        cli.exportToFile(date, stage, output);
    }

    private static void handleSearchCommand(DeadLetterBrowser cli, String[] args) {
        String exception = getArgument(args, "--exception");

        if (exception == null) {
            System.err.println("✗ Missing required argument: --exception");
            System.exit(1);
        }

        cli.search(exception);
    }

    private static void handleReplayCommand(DeadLetterBrowser cli, String[] args) {
        String date = getArgument(args, "--date");
        String stage = getArgument(args, "--stage");

        if (date == null || stage == null) {
            System.err.println("✗ Missing required arguments: --date, --stage");
            System.exit(1);
        }

        cli.replayInstructions(date, stage);
    }

    private static String getArgument(String[] args, String name) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) {
                return args[i + 1];
            }
        }
        return null;
    }

    private static void printUsage() {
        System.out.println("Dead Letter Channel CLI - Maintenance Tool");
        System.out.println();
        System.out.println("Usage:");
        System.out.println("  java DeadLetterCLI list [--date YYYY-MM-DD] [--stage STAGE_NAME]");
        System.out.println("  java DeadLetterCLI view --date YYYY-MM-DD --stage STAGE_NAME --offset OFFSET");
        System.out.println("  java DeadLetterCLI station --station-id STATION_ID");
        System.out.println("  java DeadLetterCLI stats --date YYYY-MM-DD");
        System.out.println("  java DeadLetterCLI export --date YYYY-MM-DD [--stage STAGE_NAME] --output FILE");
        System.out.println("  java DeadLetterCLI search --exception PATTERN");
        System.out.println("  java DeadLetterCLI replay --date YYYY-MM-DD --stage STAGE_NAME");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java DeadLetterCLI list --date 2026-05-22");
        System.out.println("  java DeadLetterCLI list --date 2026-05-22 --stage PARQUET_WRITE");
        System.out.println("  java DeadLetterCLI view --date 2026-05-22 --stage PARQUET_WRITE --offset 1234");
        System.out.println("  java DeadLetterCLI station --station-id 5");
        System.out.println("  java DeadLetterCLI stats --date 2026-05-22");
        System.out.println("  java DeadLetterCLI export --date 2026-05-22 --output export.jsonl");
        System.out.println("  java DeadLetterCLI search --exception IOException");
        System.out.println("  java DeadLetterCLI replay --date 2026-05-22 --stage PARQUET_WRITE");
    }
}
