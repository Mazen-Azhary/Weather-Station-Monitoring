public class HintFileManager {

    private CompactionManager c;
    private FileManager f;
    private HashMapManager hmm;

    public HintFileManager() {
        this.f   = FileManager.getInstance();
        this.hmm = HashMapManager.getInstance();
        this.c   = new CompactionManager();
    }

    public void write(FileManager fManager, String id, String value, int writesTillComp) {
        // TODO: implement
    }

    public void buildMap() {
        // TODO: implement
    }
}
