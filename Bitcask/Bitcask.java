public class Bitcask {

    private WriteManager w;
    private ReadManager r;

    public Bitcask() {
        this.w = new WriteManager();
        this.r = new ReadManager();
    }

    public String read(String stationID) {
        return  this.r.read(stationID);
    }

    public void write(String stationID, String status) {
        this.w.(stationID,status);
    }
}
