package com.example.centralstation.bitcask;

public class Bitcask {

    // Singleton — shared across the whole process.
    private static volatile Bitcask instance;

    private final WriteManager w;
    private final ReadManager  r;

    private Bitcask() {
        this.w = new WriteManager();
        this.r = new ReadManager();
    }

    /**
     * Returns the single shared Bitcask instance.
     * Uses double-checked locking for thread safety.
     */
    public static Bitcask getInstance() {
        if (instance == null) {
            synchronized (Bitcask.class) {
                if (instance == null) {
                    instance = new Bitcask();
                }
            }
        }
        return instance;
    }

    public String read(String stationID) {
        return this.r.read(stationID);
    }

    /** Synchronized so concurrent Kafka consumer threads cannot interleave writes. */
    public synchronized void write(String stationID, String status) {
        this.w.write(stationID, status);
    }
}