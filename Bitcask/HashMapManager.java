import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    // Returns the cached value for id, or null on a cache miss.
    public String read(String id) {
        return mp.get(id); // null = miss → caller falls back to HintFileManager
    }

    // Insert or overwrite a single entry.
    public void write(String id, String status) {
        mp.put(id, status);
    }

    // Bulk-load from two parallel lists (used during startup / after compaction).
    public void build(List<String> ids, List<String> statuses) {
        if (ids.size() != statuses.size()) {
            throw new IllegalArgumentException("ids and statuses lists must be the same length");
        }
        for (int i = 0; i < ids.size(); i++) {
            mp.put(ids.get(i), statuses.get(i));
        }
    }

    // Wipe the in-memory map (called before a full rebuild after compaction).
    public void clear() {
        mp.clear();
    }
}