public class Bitcask {

    private WriteManager w;
    private ReadManager r;

    public Bitcask() {
        this.w = new WriteManager();
        this.r = new ReadManager();
    }

    public String read(String stationID) {
        // TODO: implement
        return null;
    }

    public void write(String stationID, String status) {
        // TODO: implement
    }
}
