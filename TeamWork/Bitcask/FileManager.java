package com.example.centralstation.bitcask;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class FileManager {

    private static FileManager instance;
    private List<File> files;
    private File activeFile;
    private RandomAccessFile activeRAF;
    private static final int MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024; // 10 MB
    private static final String DIRECTORY;

    static {
        String envDir = System.getenv("BITCASK_DATA_DIR");
        DIRECTORY = (envDir != null && !envDir.isBlank())
                ? (envDir.endsWith("/") || envDir.endsWith("\\") ? envDir : envDir + "/")
                : "./Files/";
    }
    private static final int HEADER_BLOCK_SIZE = 1024 * 1024; // 1 MB reserved header (matches CompactionManager)

    // Binary record layout (written after the header block):
    //  [4 bytes: idLength][idLength bytes: id][4 bytes: valueLength][valueLength bytes: value]

    private FileManager() {
        files = new ArrayList<>();
        File dir = new File(DIRECTORY);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        initActiveFile();
    }

    public static FileManager getInstance() {
        if (instance == null) {
            instance = new FileManager();
        }
        return instance;
    }

    // -------------------------------------------------------------------------
    // Initialisation: pick the file with the largest name (latest timestamp)
    // as the active file, or create a new one if none exist or the latest is full.
    // -------------------------------------------------------------------------
    private void initActiveFile() {
        File dir = new File(DIRECTORY);
        File[] found = dir.listFiles((d, name) -> name.endsWith(".bin"));

        if (found != null && found.length > 0) {
            Arrays.sort(found, Comparator.comparing(File::getName).reversed());
            for (File f : found) {
                files.add(f);
            }
            File latest = found[0];
            if (latest.length() < MAX_FILE_SIZE_BYTES) {
                activeFile = latest;
            } else {
                createFile(TimestampGenerator.generateTimestamp());
            }
        } else {
            createFile(TimestampGenerator.generateTimestamp());
        }

        try {
            activeRAF = new RandomAccessFile(activeFile, "rw");
            activeRAF.seek(activeFile.length());
        } catch (IOException e) {
            throw new RuntimeException("Failed to open active file: " + activeFile.getName(), e);
        }
    }

    // -------------------------------------------------------------------------
    // Create a new binary file named after the given timestamp.
    // The file starts with a HEADER_BLOCK_SIZE zero‑filled header.
    // -------------------------------------------------------------------------
    public void createFile(String timestamp) {
        String filename = DIRECTORY + timestamp + ".bin";
        File newFile = new File(filename);
        try {
            newFile.createNewFile();
            try (RandomAccessFile raf = new RandomAccessFile(newFile, "rw")) {
                raf.write(new byte[HEADER_BLOCK_SIZE]);  // reserve header block
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create file: " + filename, e);
        }
        files.add(newFile);
        activeFile = newFile;

        try {
            if (activeRAF != null) {
                activeRAF.close();
            }
            activeRAF = new RandomAccessFile(activeFile, "rw");
            activeRAF.seek(activeFile.length());
        } catch (IOException e) {
            throw new RuntimeException("Failed to open new active file: " + filename, e);
        }
    }

    // -------------------------------------------------------------------------
    // Write a record to the active file (after the header block).
    // Returns the absolute byte offset (including header) at which the record was written.
    // Rotates to a new file automatically if the active file is full.
    // -------------------------------------------------------------------------
    public long writeRecord(String id, String value) {
        byte[] idBytes    = id.getBytes();
        byte[] valueBytes = value.getBytes();
        int recordSize    = 4 + idBytes.length + 4 + valueBytes.length;

        try {
            if (activeFile.length() + recordSize > MAX_FILE_SIZE_BYTES) {
                createFile(TimestampGenerator.generateTimestamp());
            }

            long offset = activeRAF.length();   // absolute position (includes header)
            activeRAF.seek(offset);

            activeRAF.writeInt(idBytes.length);
            activeRAF.write(idBytes);
            activeRAF.writeInt(valueBytes.length);
            activeRAF.write(valueBytes);

            return offset;
        } catch (IOException e) {
            throw new RuntimeException("Failed to write record for id: " + id, e);
        }
    }

    // -------------------------------------------------------------------------
    // Read a record from any file by scanning sequentially (fallback).
    // Skips the header block of each file.
    // -------------------------------------------------------------------------
    public String readRecord(String id) {
        for (File file : files) {
            String result = readFromFile(file, id);
            if (result != null) return result;
        }
        return null;
    }

    // -------------------------------------------------------------------------
    // Read a record from a specific file at a known absolute offset.
    // The offset must already account for the header block.
    // -------------------------------------------------------------------------
    public String readRecord(String filename, int offset, int size, String id) {
        File file = new File(DIRECTORY + filename);
        if (!file.exists()) return null;

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(offset);

            int    idLen    = raf.readInt();
            byte[] idBytes  = new byte[idLen];
            raf.readFully(idBytes);
            String storedId = new String(idBytes);

            if (!storedId.equals(id)) return null;

            int    valLen    = raf.readInt();
            byte[] valBytes  = new byte[valLen];
            raf.readFully(valBytes);
            return new String(valBytes);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read record at offset " + offset + " in " + filename, e);
        }
    }

    // -------------------------------------------------------------------------
    // Delete a file by name and remove it from the in-memory list.
    // -------------------------------------------------------------------------
    public void deleteFile(String filename) {
        File target = new File(DIRECTORY + filename);
        files.removeIf(f -> f.getName().equals(filename));
        if (target.exists()) {
            target.delete();
        }
    }

    // -------------------------------------------------------------------------
    // Open a file by name and set it as the active file.
    // -------------------------------------------------------------------------
    public void openFile(String filename) {
        File target = new File(DIRECTORY + filename);
        if (!target.exists()) {
            throw new RuntimeException("File not found: " + filename);
        }
        try {
            if (activeRAF != null) {
                activeRAF.close();
            }
            activeFile = target;
            activeRAF  = new RandomAccessFile(activeFile, "rw");
            activeRAF.seek(activeFile.length());
        } catch (IOException e) {
            throw new RuntimeException("Failed to open file: " + filename, e);
        }
    }

    // -------------------------------------------------------------------------
    // Close the RandomAccessFile handle for the given filename.
    // -------------------------------------------------------------------------
    public void closeFile(String filename) {
        if (activeFile != null && activeFile.getName().equals(filename)) {
            try {
                if (activeRAF != null) {
                    activeRAF.close();
                    activeRAF = null;
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to close file: " + filename, e);
            }
            activeFile = null;
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------
    public File getActiveFile() {
        return activeFile;
    }

    public List<File> getFiles() {
        return files;
    }

    public int getMaxFileSizeBytes() {
        return MAX_FILE_SIZE_BYTES;
    }

    public String getDirectory() {
        return DIRECTORY;
    }

    public void registerFile(File file) {
        if (!files.contains(file)) {
            files.add(file);
        }
    }

    public void reloadActiveFile() {
        if (files.isEmpty()) {
            createFile(TimestampGenerator.generateTimestamp());
            return;
        }
        files.sort(Comparator.comparing(File::getName).reversed());
        File latest = files.get(0);
        try {
            if (activeRAF != null) {
                activeRAF.close();
            }
            if (latest.length() < MAX_FILE_SIZE_BYTES) {
                activeFile = latest;
            } else {
                createFile(TimestampGenerator.generateTimestamp());
                return;
            }
            activeRAF = new RandomAccessFile(activeFile, "rw");
            activeRAF.seek(activeFile.length());
        } catch (IOException e) {
            throw new RuntimeException("Failed to reload active file after compaction", e);
        }
    }

    // -------------------------------------------------------------------------
    // Scan a single file sequentially (starting after the header) for a matching id.
    // -------------------------------------------------------------------------
    private String readFromFile(File file, String id) {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(HEADER_BLOCK_SIZE);   // skip the reserved header
            while (raf.getFilePointer() < raf.length()) {
                int    idLen    = raf.readInt();
                byte[] idBytes  = new byte[idLen];
                raf.readFully(idBytes);
                String storedId = new String(idBytes);

                int    valLen   = raf.readInt();
                byte[] valBytes = new byte[valLen];
                raf.readFully(valBytes);

                if (storedId.equals(id)) {
                    return new String(valBytes);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read from file: " + file.getName(), e);
        }
        return null;
    }
}