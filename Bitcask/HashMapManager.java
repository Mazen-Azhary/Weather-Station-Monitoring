import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class HashMapManager {

    private static HashMapManager instance;
    private Map<String, String> mp;

    private HashMapManager() {
        mp = new HashMap<>();
    }

    public static HashMapManager getInstance() {
        if (instance == null) {
            instance = new HashMapManager();
        }
        return instance;
    }

    public String read(String id) {
        // TODO: implement
        return null;
    }

    public void write(String id, String status) {
        // TODO: implement
    }

    public void build(List<String> ids, List<String> statuses) {
        // TODO: implement
    }
}
