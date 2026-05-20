public class WriteManager {

    private FileManager f;
    private HintFileManager hfm;
    private HashMapManager hmm;
    private int writesLeftTillCompaction;

    public WriteManager() {
        this.f   = FileManager.getInstance();
        this.hmm = HashMapManager.getInstance();
        this.hfm = new HintFileManager();
        // TODO: initialize writesLeftTillCompaction
    }

    public void buildMap(HashMapManager hmm) {
        // TODO: implement
    }

    public void write(String stationId, String status) {
        // TODO: implement
    }
}
