package com.example.centralstation.bitcask;

import java.io.*;
import java.util.*;

public class HintFileManager {

    // -------------------------------------------------------------------------
    // Single global hint file: ./Files/global.hint
    //
    // Binary format per entry (append-only, last write for an id wins):
    //   [4B  idLen      ]
    //   [idLen bytes: id]
    //   [4B  filenameLen]
    //   [filenameLen bytes: filename]
    //   [8B  offset     ]   ← byte offset in the data file
    //   [4B  size       ]   ← total byte length of the record in the data file
    //
    // On a HashMapManager cache-miss, HintFileManager looks up its in-memory
    // index, seeks directly to (filename, offset, size) in the data file,
    // fetches the value, warms HashMapManager, and returns the value.
    // -------------------------------------------------------------------------

    private static final String HINT_FILENAME = "global.hint";

    private static HintFileManager instance;

    private CompactionManager c;
    private FileManager       f;
    private HashMapManager    hmm;

    // In-memory mirror of the hint file — last entry per id wins (same as disk).
    // Value is a HintEntry: (filename, offset, size)
    private final Map<String, HintEntry> hintIndex = new LinkedHashMap<>();

    private static class HintEntry {
        String filename;
        long   offset;
        int    size;

        HintEntry(String filename, long offset, int size) {
            this.filename = filename;
            this.offset   = offset;
            this.size     = size;
        }
    }

    // -------------------------------------------------------------------------
    // Singleton
    // -------------------------------------------------------------------------
    private HintFileManager() {
        this.f   = FileManager.getInstance();
        this.hmm = HashMapManager.getInstance();
        this.c   = new CompactionManager();
        buildMap(); // warm hintIndex and HashMapManager from disk on startup
    }

    public static HintFileManager getInstance() {
        if (instance == null) {
            instance = new HintFileManager();
        }
        return instance;
    }

    // -------------------------------------------------------------------------
    // Read path (called by ReadManager).
    //
    // 1. Check HashMapManager hot cache → return immediately on hit.
    // 2. Cache miss → look up hintIndex for (filename, offset, size).
    // 3. Direct seek into the data file via FileManager.readRecord(filename, offset, size, id).
    // 4. Populate HashMapManager so the next read is a cache hit.
    // 5. Return value, or null if genuinely not found anywhere.
    // -------------------------------------------------------------------------
    public String read(String id) {
        // Fast path
        String cached = hmm.read(id);
        if (cached != null) return cached;

        // Slow path
        HintEntry entry = hintIndex.get(id);
        if (entry == null) return null; // not found anywhere

        String value = f.readRecord(entry.filename, (int) entry.offset, entry.size, id);
        if (value != null) {
            hmm.write(id, value); // warm cache for next time
        }
        return value;
    }

    // -------------------------------------------------------------------------
    // Write path (called by WriteManager after every successful data file write).
    //
    // Parameters:
    //   activeFilename  - name of the data file the record was written to
    //   id              - station id
    //   value           - status value
    //   offset          - byte offset returned by FileManager.writeRecord()
    //   size            - total byte size of the written record
    //   writesTillComp  - remaining writes before compaction; triggers compact()
    //                     when it reaches 0
    // -------------------------------------------------------------------------
    public void write(String activeFilename, String id, String value,
                      long offset, int size, int writesTillComp) {

        // 1. Update in-memory hint index
        hintIndex.put(id, new HintEntry(activeFilename, offset, size));

        // 2. Warm HashMapManager hot cache
        hmm.write(id, value);

        // 3. Append entry to hint file on disk
        appendToHintFile(id, activeFilename, offset, size);

        // 4. Trigger compaction if countdown reached zero
        if (writesTillComp <= 0) {
            c.compact();
            // After compaction data files have new names → rebuild hint file
            // from the index (CompactionManager calls updateFilename() before
            // returning so hintIndex already has the correct filenames).
            rebuildHintFile();
        }
    }

    // -------------------------------------------------------------------------
    // Scan the hint file from disk and rebuild the in-memory hintIndex.
    // Also bulk-loads HashMapManager by doing a direct-seek read for each entry.
    //
    // Called:
    //   • On startup (constructor)
    //   • After compaction rewrites the hint file
    // -------------------------------------------------------------------------
    public void buildMap() {
        hintIndex.clear();
        hmm.clear();

        File hintFile = new File(FileManager.getInstance().getDirectory() + HINT_FILENAME);
        if (!hintFile.exists()) return;

        // Scan whole file; last occurrence of each id wins (append-only log)
        try (RandomAccessFile raf = new RandomAccessFile(hintFile, "r")) {
            while (raf.getFilePointer() < raf.length()) {
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

                hintIndex.put(id, new HintEntry(filename, offset, size));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read hint file during buildMap", e);
        }

        // Populate HashMapManager from the deduplicated hintIndex
        List<String> ids      = new ArrayList<>();
        List<String> statuses = new ArrayList<>();

        for (Map.Entry<String, HintEntry> e : hintIndex.entrySet()) {
            String id    = e.getKey();
            HintEntry he = e.getValue();
            String value = f.readRecord(he.filename, (int) he.offset, he.size, id);
            if (value != null) {
                ids.add(id);
                statuses.add(value);
            }
        }
        hmm.build(ids, statuses);
    }

    // -------------------------------------------------------------------------
    // After compaction renames a merge file to its final timestamp name, call
    // this so the in-memory index stays in sync with the new filenames.
    // -------------------------------------------------------------------------
    public void updateFilename(String oldFilename, String newFilename) {
        for (HintEntry entry : hintIndex.values()) {
            if (entry.filename.equals(oldFilename)) {
                entry.filename = newFilename;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Rewrite the hint file on disk from the current in-memory index.
    // One entry per id (deduplicated); called after compaction.
    // -------------------------------------------------------------------------
    public void rebuildHintFile() {
        File hintFile = new File(FileManager.getInstance().getDirectory() + HINT_FILENAME);
        if (hintFile.exists()) hintFile.delete();
        try {
            hintFile.createNewFile();
            try (RandomAccessFile raf = new RandomAccessFile(hintFile, "rw")) {
                for (Map.Entry<String, HintEntry> e : hintIndex.entrySet()) {
                    writeEntry(raf, e.getKey(), e.getValue());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to rebuild hint file after compaction", e);
        }
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private void appendToHintFile(String id, String filename, long offset, int size) {
        File hintFile = new File(FileManager.getInstance().getDirectory() + HINT_FILENAME);
        try {
            if (!hintFile.exists()) hintFile.createNewFile();
            try (RandomAccessFile raf = new RandomAccessFile(hintFile, "rw")) {
                raf.seek(raf.length()); // always append
                writeEntry(raf, id, new HintEntry(filename, offset, size));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to append to hint file for id: " + id, e);
        }
    }

    // Binary layout: [4B idLen][id][4B filenameLen][filename][8B offset][4B size]
    private void writeEntry(RandomAccessFile raf, String id, HintEntry entry) throws IOException {
        byte[] idBytes = id.getBytes();
        byte[] fnBytes = entry.filename.getBytes();

        raf.writeInt(idBytes.length);
        raf.write(idBytes);
        raf.writeInt(fnBytes.length);
        raf.write(fnBytes);
        raf.writeLong(entry.offset);
        raf.writeInt(entry.size);
    }
}