import java.io.File;
import java.util.List;

public class FileManager {

    private static FileManager instance;
    private List<File> files;
    private File activeFile;

    private FileManager() {
        // TODO: initialize fields
    }

    public static FileManager getInstance() {
        if (instance == null) {
            instance = new FileManager();
        }
        return instance;
    }

    public String readRecord(String id) {
        // TODO: implement
        return null;
    }

    public String readRecord(String filename, int offset, int size, String id) {
        // TODO: implement
        return null;
    }

    public void deleteFile(String filename) {
        // TODO: implement
    }

    public void openFile(String filename) {
        // TODO: implement
    }

    public void closeFile(String filename) {
        // TODO: implement
    }

    public void createFile(long timestamp) {
        // TODO: implement
    }
}
