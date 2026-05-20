public class ReadManager {

    private HintFileManager hfm;

    public ReadManager() {
        this.hfm = HintFileManager.getInstance();
    }

    // Read path:
    //   1. HintFileManager checks HashMapManager first.
    //   2. On a miss it seeks directly into the correct data file using the
    //      hint index (filename + offset + size), warms the cache, and returns.
    //   3. Returns null if the id has never been written.
    public String read(String stationID) {
        return hfm.read(stationID);
    }
}