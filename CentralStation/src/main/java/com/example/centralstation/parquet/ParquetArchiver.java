package com.example.centralstation.parquet;

import com.example.centralstation.model.WeatherReading;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects WeatherReading records in memory and writes them to
 * partitioned Parquet files in batches of 10,000 records.
 *
 * Output path layout:
 *   /app/data/parquet/year=YYYY/month=MM/day=DD/station_id=N/part-NNNNN.parquet
 *
 * A background thread handles all disk writes so the Kafka
 * consumer is never blocked by I/O.
 */
public class ParquetArchiver {

    // ---- Avro schema: defines the Parquet columns ----
    private static final Schema SCHEMA = SchemaBuilder.record("WeatherRecord")
            .namespace("com.example.centralstation")
            .fields()
                .requiredLong("station_id")
                .requiredLong("s_no")
                .requiredString("battery_status")
                .requiredLong("status_timestamp")
                .requiredInt("humidity")
                .requiredInt("temperature")
                .requiredInt("wind_speed")
                .requiredInt("year")
                .requiredInt("month")
                .requiredInt("day")
            .endRecord();

    private static final AtomicLong fileCounter = new AtomicLong(0);

    private final BlockingQueue<WeatherReading> queue;
    private final String outputPath;
    private final int    batchSize;
    private volatile boolean running = true;
    private Thread writerThread;

    public ParquetArchiver(String outputPath, int batchSize) {
        this.outputPath = outputPath;
        this.batchSize  = batchSize;
        this.queue      = new ArrayBlockingQueue<>(batchSize * 2);
    }

    // ---- Lifecycle ----

    /** Start the background writer thread. */
    public void start() {
        writerThread = new Thread(this::writeLoop, "parquet-writer");
        writerThread.setDaemon(false);
        writerThread.start();
        System.out.println("Parquet archiver started. Output path: " + outputPath);
    }

    /** Add a record to be archived. Drops if the queue is full. */
    public void add(WeatherReading reading) {
        if (!queue.offer(reading)) {
            System.err.println("Archiver queue full – dropping record for station " + reading.getStationId());
        }
    }

    /** Stop the archiver and flush any buffered records. */
    public void stop() {
        running = false;
        try {
            writerThread.join(60_000); // wait up to 60 sec for final flush
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ---- Background writer loop ----

    private void writeLoop() {
        List<WeatherReading> batch = new ArrayList<>(batchSize);

        while (running || !queue.isEmpty()) {
            try {
                WeatherReading r = queue.poll(500, TimeUnit.MILLISECONDS);
                if (r != null) {
                    batch.add(r);
                }
                if (batch.size() >= batchSize) {
                    writeBatch(batch);
                    batch.clear();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break; // exit loop, then drain below
            }
        }

        // Always flush whatever is left (even after interrupt or stop)
        queue.drainTo(batch);
        if (!batch.isEmpty()) {
            System.out.println("Flushing " + batch.size() + " remaining records...");
            writeBatch(batch);
        }
    }

    // ---- Parquet file writing ----

    /**
     * Groups records by station_id, then writes a separate Parquet file
     * for each station so partitioning by station_id is correct.
     */
    private void writeBatch(List<WeatherReading> records) {
        if (records.isEmpty()) return;

        // Group records by station_id (one batch may contain many stations)
        Map<Long, List<WeatherReading>> byStation = new HashMap<>();
        for (WeatherReading r : records) {
            byStation.computeIfAbsent(r.getStationId(), k -> new ArrayList<>()).add(r);
        }

        // Write a separate Parquet file for each station
        for (Map.Entry<Long, List<WeatherReading>> entry : byStation.entrySet()) {
            writeStationFile(entry.getValue());
        }
    }

    /** Writes one Parquet file for a single station's records. */
    private void writeStationFile(List<WeatherReading> records) {
        WeatherReading first = records.get(0);

        // Compute partition from this record's timestamp
        ZonedDateTime dt = Instant.ofEpochSecond(first.getStatusTimestamp()).atZone(ZoneOffset.UTC);
        int year  = dt.getYear();
        int month = dt.getMonthValue();
        int day   = dt.getDayOfMonth();

        // Build directory: year=YYYY/month=MM/day=DD/station_id=N
        String dir = String.format("%s/year=%d/month=%02d/day=%02d/station_id=%d",
                outputPath, year, month, day, first.getStationId());

        try {
            Files.createDirectories(Paths.get(dir));

            String fileName = String.format("part-%05d.parquet", fileCounter.incrementAndGet());
            String fullPath = dir + "/" + fileName;

            // Write using LocalOutputFile — no Hadoop FileSystem needed
            LocalOutputFile outputFile = new LocalOutputFile(Paths.get(fullPath));
            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                    .<GenericRecord>builder(outputFile)
                    .withSchema(SCHEMA)
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                    .build()) {

                for (WeatherReading r : records) {
                    writer.write(toAvroRecord(r, year, month, day));
                }
            }

            System.out.println("Wrote " + records.size() + " records → " + fullPath);

        } catch (Throwable e) {
            System.err.println("PARQUET WRITE FAILED for station " + first.getStationId() + ": " + e);
            e.printStackTrace();
        }
    }

    /** Convert a WeatherReading to an Avro GenericRecord. */
    private static GenericRecord toAvroRecord(WeatherReading r, int year, int month, int day) {
        GenericRecord rec = new GenericData.Record(SCHEMA);
        rec.put("station_id",       r.getStationId());
        rec.put("s_no",             r.getSequenceNumber());
        rec.put("battery_status",   r.getBatteryStatus() != null ? r.getBatteryStatus() : "unknown");
        rec.put("status_timestamp", r.getStatusTimestamp());
        rec.put("humidity",         r.getWeather() != null ? r.getWeather().getHumidity()    : 0);
        rec.put("temperature",      r.getWeather() != null ? r.getWeather().getTemperature() : 0);
        rec.put("wind_speed",       r.getWeather() != null ? r.getWeather().getWindSpeed()   : 0);
        rec.put("year",  year);
        rec.put("month", month);
        rec.put("day",   day);
        return rec;
    }
}
