// NodeCache.java
//
// Ένα απλό LRU cache για Paged R*-tree κόμβους.
// Χρησιμοποιεί LinkedHashMap με access-order για να κρατάει σταθερό αριθμό κόμβων.
// Όταν ένας «eldest» κόμβος πρέπει να εκδιωχθεί, αν είναι dirty γράφεται κατευθείαν στον δίσκο.

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class NodeCache {
    // Πόσες σελίδες (nodes) κρατάμε στη μνήμη
    private final int capacity;
    private final IndexFile indexFile;

    // Κάθε CacheEntry περιέχει τον κόμβο και ένα flag dirty
    private static class CacheEntry {
        Node node;
        boolean dirty;
        CacheEntry(Node node, boolean dirty) {
            this.node = node;
            this.dirty = dirty;
        }
    }

    // Η LinkedHashMap κρατάει <pageId, CacheEntry> με accessOrder=true ώστε να συμπεριφέρεται LRU
    private final LinkedHashMap<Integer, CacheEntry> map;

    public NodeCache(IndexFile indexFile, int capacity) {
        this.indexFile = indexFile;
        this.capacity  = capacity;
        this.map = new LinkedHashMap<Integer, CacheEntry>(capacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, CacheEntry> eldest) {
                if (size() > NodeCache.this.capacity) {
                    CacheEntry ce = eldest.getValue();
                    if (ce.dirty) {
                        // Όταν εκδιώκεται, γράφουμε πίσω στον δίσκο
                        try {
                            NodeCache.this.indexFile.writeNode(eldest.getKey(), ce.node);
                        } catch (IOException ex) {
                            // Εκτυπώνουμε το σφάλμα, αλλά δεν το πετάμε περισσότερο
                            ex.printStackTrace();
                        }
                    }
                    return true;
                }
                return false;
            }
        };
    }

    /**
     * Επιστρέφει τον κόμβο με το δοσμένο pageId:
     *  - Αν υπάρχει ήδη στη cache, επιστρέφεται αμέσως (cache hit).
     *  - Διαφορετικά (cache miss), κατεβαίνει από δίσκο, τον βάζει στην cache (clean) και επιστρέφεται.
     */
    public Node fetch(int pageId) throws IOException {
        if (pageId < 0) return null;
        CacheEntry ce = map.get(pageId);
        if (ce != null) {
            return ce.node;
        }
        Node fromDisk = indexFile.readNode(pageId);
        if (fromDisk != null) {
            map.put(pageId, new CacheEntry(fromDisk, false));
        }
        return fromDisk;
    }

    /**
     * Σηματοδοτεί ότι ο κόμβος άλλαξε (dirty) και τον αποθηκεύει στην cache.
     * Αν δεν υπάρχει ήδη, εισάγεται με dirty=true, αλλιώς απλώς σηματοδοτείται dirty.
     */
    public void store(Node node) {
        int pageId = node.getPageId();
        CacheEntry ce = map.get(pageId);
        if (ce != null) {
            ce.node = node;
            ce.dirty = true;
        } else {
            map.put(pageId, new CacheEntry(node, true));
        }
    }

    /**
     * Ο-editor-back: Γράφει στο δίσκο όλους τους κόμβους που παραμένουν στην cache και είναι dirty.
     * Καλείται συνήθως στο τέλος των build/insert loops, για να εξασφαλίσουμε ότι όλα έχουν γραφτεί.
     */
    public void flushAll() throws IOException {
        for (Map.Entry<Integer, CacheEntry> entry : map.entrySet()) {
            CacheEntry ce = entry.getValue();
            if (ce.dirty) {
                indexFile.writeNode(entry.getKey(), ce.node);
                ce.dirty = false;
            }
        }
    }

    /**
     * Διαγράφει *ολόκληρη* την cache (χωρίς write-back), π.χ. σε περίπτωση rollback.
     */
    public void clear() {
        map.clear();
    }
}
