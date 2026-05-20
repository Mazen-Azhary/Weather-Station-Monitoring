import java.io.*;
import java.util.*;

public class CompactionManager {

    // Header is a fixed-size CSV block at byte 0 of every merge file.
    // Format: "id1:offset1,id2:offset2,..."
    // Unused bytes in the reserved block are zero-padded.
    // HEADER_BLOCK_SIZE must be large enough to hold all id:offset pairs for one merge file.
    private static final int HEADER_BLOCK_SIZE = 64 * 1024; // 64 KB reserved for header

    private FileManager f;

    public CompactionManager() {
        this.f = FileManager.getInstance();
    }

    // -------------------------------------------------------------------------
    // Main compaction entry point.
    // -------------------------------------------------------------------------
    public void compact() {
        // Step 1: scan all files oldest→newest, keep only the latest value per id.
        //         Files are named by timestamp; sort ascending = chronological order.
        List<File> dataFiles = new ArrayList<>(f.getFiles());
        dataFiles.sort(Comparator.comparing(File::getName)); // ascending = oldest first

        // latest value seen for each id
        Map<String, String> latestValues = new LinkedHashMap<>();

        for (File file : dataFiles) {
            scanFile(file, latestValues);
        }

        if (latestValues.isEmpty()) return;

        // Step 2: write merge files
        //         Each merge file = [HEADER_BLOCK_SIZE bytes reserved] + binary records
        //         Binary record format (same as FileManager): [4B idLen][id][4B valLen][val]
        int maxRecordSection = f.getMaxFileSizeBytes() - HEADER_BLOCK_SIZE;

        List<File> mergeFiles       = new ArrayList<>();
        List<Map<String, Long>> mergeHeaders = new ArrayList<>(); // id → offset within file

        int    mergeIndex   = 1;
        File   mergeFile    = mergeFile(mergeIndex);
        RandomAccessFile raf = openMergeFile(mergeFile);
        Map<String, Long> headerMap = new LinkedHashMap<>();

        for (Map.Entry<String, String> entry : latestValues.entrySet()) {
            String id    = entry.getKey();
            String value = entry.getValue();

            byte[] idBytes  = id.getBytes();
            byte[] valBytes = value.getBytes();
            int    recSize  = 4 + idBytes.length + 4 + valBytes.length;

            try {
                // Would this record push the record section past the size limit?
                long currentRecordSectionSize = raf.length() - HEADER_BLOCK_SIZE;
                if (currentRecordSectionSize + recSize > maxRecordSection) {
                    // Flush header, close current merge file, open a new one
                    writeHeader(raf, headerMap);
                    raf.close();
                    mergeFiles.add(mergeFile);
                    mergeHeaders.add(headerMap);

                    mergeIndex++;
                    mergeFile = mergeFile(mergeIndex);
                    raf       = openMergeFile(mergeFile);
                    headerMap = new LinkedHashMap<>();
                }

                // Record the offset (relative to the start of the file)
                long offset = raf.getFilePointer();
                headerMap.put(id, offset);

                // Write record
                raf.writeInt(idBytes.length);
                raf.write(idBytes);
                raf.writeInt(valBytes.length);
                raf.write(valBytes);

            } catch (IOException e) {
                throw new RuntimeException("Failed writing merge file: " + mergeFile.getName(), e);
            }
        }

        // Flush header for the last (or only) merge file
        try {
            writeHeader(raf, headerMap);
            raf.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed closing last merge file", e);
        }
        mergeFiles.add(mergeFile);
        mergeHeaders.add(headerMap);

        // Step 3: delete all old data files via FileManager
        for (File old : dataFiles) {
            f.deleteFile(old.getName());
        }

        // Step 4: rename each merge file to a fresh timestamp and register with FileManager
        for (int i = 0; i < mergeFiles.size(); i++) {
            File   mf          = mergeFiles.get(i);
            long   ts          = System.currentTimeMillis() + i; // +i ensures unique names
            String newName     = ts + ".bin";
            File   renamed     = new File(f.getDirectory() + newName);
            mf.renameTo(renamed);
            f.registerFile(renamed);
        }

        // Step 5: tell FileManager to re-evaluate its active file
        f.reloadActiveFile();
    }

    // -------------------------------------------------------------------------
    // Scan a data file from start to finish and update the latestValues map.
    // Binary format: [4B idLen][id][4B valLen][val] repeated.
    // -------------------------------------------------------------------------
    private void scanFile(File file, Map<String, String> latestValues) {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            while (raf.getFilePointer() < raf.length()) {
                int    idLen   = raf.readInt();
                byte[] idBytes = new byte[idLen];
                raf.readFully(idBytes);

                int    valLen   = raf.readInt();
                byte[] valBytes = new byte[valLen];
                raf.readFully(valBytes);

                // Always overwrite → last write in chronological order wins
                latestValues.put(new String(idBytes), new String(valBytes));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed scanning file: " + file.getName(), e);
        }
    }

    // -------------------------------------------------------------------------
    // Create a merge file in the Files/ directory and reserve the header block.
    // -------------------------------------------------------------------------
    private File mergeFile(int index) {
        return new File(f.getDirectory() + "merge" + index + ".bin");
    }

    private RandomAccessFile openMergeFile(File file) {
        try {
            if (file.exists()) file.delete();
            file.createNewFile();
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            // Reserve header block with zero bytes
            raf.write(new byte[HEADER_BLOCK_SIZE]);
            // RAF pointer is now at HEADER_BLOCK_SIZE — ready to write records
            return raf;
        } catch (IOException e) {
            throw new RuntimeException("Failed creating merge file: " + file.getName(), e);
        }
    }

    // -------------------------------------------------------------------------
    // Seek back to byte 0 and write the CSV header, zero-padding the remainder.
    // Format: "id1:offset1,id2:offset2,..."
    // -------------------------------------------------------------------------
    private void writeHeader(RandomAccessFile raf, Map<String, Long> headerMap) throws IOException {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, Long> e : headerMap.entrySet()) {
            if (!first) sb.append(',');
            sb.append(e.getKey()).append(':').append(e.getValue());
            first = false;
        }

        byte[] headerBytes = sb.toString().getBytes();
        if (headerBytes.length > HEADER_BLOCK_SIZE) {
            throw new RuntimeException("Header exceeds reserved block size — increase HEADER_BLOCK_SIZE");
        }

        long savedPos = raf.getFilePointer();
        raf.seek(0);
        raf.write(headerBytes);
        // Zero-pad the rest of the header block
        int remaining = HEADER_BLOCK_SIZE - headerBytes.length;
        raf.write(new byte[remaining]);
        raf.seek(savedPos);
    }
}