package com.example.centralstation.bitcask;

public class WriteManager {

    private static final int WRITES_TILL_COMPACTION = 1000;
    private FileManager     f;
    private HintFileManager hfm;
    private HashMapManager  hmm;
    private int             writesLeftTillCompaction;
    public WriteManager() {
        this.f  = FileManager.getInstance();
        this.hmm = HashMapManager.getInstance();
        this.hfm = HintFileManager.getInstance();
        this.writesLeftTillCompaction = WRITES_TILL_COMPACTION;
    }

    // Delegates entirely to HintFileManager, which scans the hint file on disk
    // and bulk-loads both hintIndex and HashMapManager.
    public void buildMap() {
        hfm.buildMap();
    }

    // 1. Write the record to the active data file (rotates file if full).
    // 2. Decrement the compaction countdown.
    // 3. Hand off to HintFileManager.write(), which:
    //      - updates the hint index + hint file on disk
    //      - warms HashMapManager
    //      - triggers compaction when writesLeftTillCompaction hits 0
    public void write(String stationId, String status) {
        // Write to data file; get back the byte offset and compute record size
        long   offset     = f.writeRecord(stationId, status);
        byte[] idBytes    = stationId.getBytes();
        byte[] valBytes   = status.getBytes();
        int recordSize = 4+idBytes.length + 4+valBytes.length;

        // Decrement before passing so HintFileManager sees the updated countdown
        writesLeftTillCompaction--;

        String activeFilename = f.getActiveFile().getName();
        hfm.write(activeFilename, stationId, status, offset, recordSize, writesLeftTillCompaction);

        // Reset counter after compaction (compaction fires inside hfm.write when <= 0)
        if (writesLeftTillCompaction <= 0) {
            writesLeftTillCompaction = WRITES_TILL_COMPACTION;
        }
    }
}