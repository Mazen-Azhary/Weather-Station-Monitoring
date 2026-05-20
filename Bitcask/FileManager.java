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
    private static final String DIRECTORY = "./Files/";

    // Binary record layout:
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
            // Sort descending by filename (filenames are timestamps → largest = latest)
            Arrays.sort(found, Comparator.comparing(File::getName).reversed());
            for (File f : found) {
                files.add(f);
            }
            File latest = found[0]; // largest filename
            if (latest.length() < MAX_FILE_SIZE_BYTES) {
                activeFile = latest;
            } else {
                createFile(System.currentTimeMillis());
            }
        } else {
            createFile(System.currentTimeMillis());
        }

        // Open a RandomAccessFile handle for the active file (append mode)
        try {
            activeRAF = new RandomAccessFile(activeFile, "rw");
            activeRAF.seek(activeFile.length()); // position at end for appending
        } catch (IOException e) {
            throw new RuntimeException("Failed to open active file: " + activeFile.getName(), e);
        }
    }

    // -------------------------------------------------------------------------
    // Create a new binary file named after the given timestamp and set it active.
    // -------------------------------------------------------------------------
    public void createFile(long timestamp) {
        String filename = DIRECTORY + timestamp + ".bin";
        File newFile = new File(filename);
        try {
            newFile.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create file: " + filename, e);
        }
        files.add(newFile);
        activeFile = newFile;

        // Re-open the RAF handle for the new active file
        try {
            if (activeRAF != null) {
                activeRAF.close();
            }
            activeRAF = new RandomAccessFile(activeFile, "rw");
        } catch (IOException e) {
            throw new RuntimeException("Failed to open new active file: " + filename, e);
        }
    }

    // -------------------------------------------------------------------------
    // Write a record to the active file.
    // Returns the byte offset at which the record was written.
    // Rotates to a new file automatically if the active file is full.
    // Binary format: [4B idLen][id bytes][4B valueLen][value bytes]
    // -------------------------------------------------------------------------
    public long writeRecord(String id, String value) {
        byte[] idBytes    = id.getBytes();
        byte[] valueBytes = value.getBytes();
        int recordSize    = 4 + idBytes.length + 4 + valueBytes.length;

        // Rotate if needed
        try {
            if (activeFile.length() + recordSize > MAX_FILE_SIZE_BYTES) {
                createFile(System.currentTimeMillis());
            }

            long offset = activeRAF.length();
            activeRAF.seek(offset);

            // Write id
            activeRAF.writeInt(idBytes.length);
            activeRAF.write(idBytes);

            // Write value
            activeRAF.writeInt(valueBytes.length);
            activeRAF.write(valueBytes);

            return offset;
        } catch (IOException e) {
            throw new RuntimeException("Failed to write record for id: " + id, e);
        }
    }

    // -------------------------------------------------------------------------
    // Read a record from the active file by scanning sequentially for the id.
    // Returns the value string, or null if not found.
    // -------------------------------------------------------------------------
    public String readRecord(String id) {
        for (File file : files) {
            String result = readFromFile(file, id);
            if (result != null) return result;
        }
        return null;
    }

    // -------------------------------------------------------------------------
    // Read a record from a specific file at a known offset and size.
    // The 'offset' is the byte position of the record start.
    // The 'size'   is the total byte length of the record.
    // 'id' is used to verify the record matches (integrity check).
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

            if (!storedId.equals(id)) return null; // mismatch guard

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
    // If it is the active file, also clear the activeFile reference.
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

    // Scan a single file sequentially for a matching id; returns value or null.
    private String readFromFile(File file, String id) {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
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

    // -------------------------------------------------------------------------
    // Helpers used by CompactionManager
    // -------------------------------------------------------------------------

    /** Expose the configured max file size so CompactionManager can split correctly. */
    public int getMaxFileSizeBytes() {
        return MAX_FILE_SIZE_BYTES;
    }

    /** Expose the working directory so CompactionManager can place merge files there. */
    public String getDirectory() {
        return DIRECTORY;
    }

    /**
     * Register an already-existing file (e.g. a renamed merge file) into the
     * in-memory list without touching it on disk.
     */
    public void registerFile(File file) {
        if (!files.contains(file)) {
            files.add(file);
        }
    }

    /**
     * After compaction renames merge files to proper timestamps, call this to
     * re-evaluate which file should be active (same logic as initActiveFile but
     * without re-scanning the directory — the list is already up to date).
     */
    public void reloadActiveFile() {
        if (files.isEmpty()) {
            createFile(System.currentTimeMillis());
            return;
        }

        // Sort descending by name; largest timestamp = most recent
        files.sort(Comparator.comparing(File::getName).reversed());
        File latest = files.get(0);

        try {
            if (activeRAF != null) {
                activeRAF.close();
            }
            if (latest.length() < MAX_FILE_SIZE_BYTES) {
                activeFile = latest;
            } else {
                createFile(System.currentTimeMillis());
                return;
            }
            activeRAF = new RandomAccessFile(activeFile, "rw");
            activeRAF.seek(activeFile.length());
        } catch (IOException e) {
            throw new RuntimeException("Failed to reload active file after compaction", e);
        }
    }
}