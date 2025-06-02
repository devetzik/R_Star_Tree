// Node.java
//
// Κλάση που αναπαριστά έναν κόμβο στο R*-tree. Περιέχει λίστα με Entry,
// το MBR που καλύπτει τις εγγραφές του, δείκτη στον γονέα, επίπεδο (0=leaf),
// κ.λπ.

import java.util.ArrayList;
import java.util.List;

public class Node {
    private final int level;          // 0 = leaf, >0 = internal
    private final boolean isLeaf;
    private final List<Entry> entries;
    private MBR mbr;                  // MBR που καλύπτει όλα τα entries
    private Node parent;              // null αν είναι root

    /**
     * @param level   Το επίπεδο (0=φυλλο, >0 εσωτερικός).
     * @param isLeaf  true αν είναι leaf.
     */
    public Node(int level, boolean isLeaf) {
        this.level = level;
        this.isLeaf = isLeaf;
        this.entries = new ArrayList<>();
        this.mbr = null;
        this.parent = null;
    }

    public int getLevel() {
        return level;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public List<Entry> getEntries() {
        return entries;
    }

    public MBR getMBR() {
        return mbr == null ? null : mbr.clone();
    }

    public Node getParent() {
        return parent;
    }

    public void setParent(Node p) {
        this.parent = p;
    }

    /**
     * Ενημερώνει το MBR του κόμβου ώστε να περιλάβει το νέοRect (τεμνόμενο με υφιστάμενο).
     * Αν δεν υπάρχει προηγούμενο mbr, το θέτει ίσο με αυτό που περάσαμε.
     */
    public void updateMBR(MBR rect) {
        if (mbr == null) {
            mbr = rect.clone();
        } else {
            mbr = MBR.union(mbr, rect);
        }
        // Ενημέρωση αναδρομικά προς τα πάνω
        if (parent != null) {
            parent.recomputeMBRUpward();
        }
    }

    /**
     * Επαναϋπολογίζει (recompute) το MBR βασισμένο σε όλα τα entries. Καλείται
     * όταν έσβησα/έβγαλα ένα entry ή μετά από split/σειρά εις άτοπον.
     */
    public void recomputeMBRUpward() {
        MBR agg = null;
        for (Entry e : entries) {
            if (agg == null) {
                agg = e.getMBR().clone();
            } else {
                agg = MBR.union(agg, e.getMBR());
            }
        }
        mbr = agg;
        if (parent != null) {
            parent.recomputeMBRUpward();
        }
    }

    /**
     * Προσθέτει ένα entry στον κόμβο.
     */
    public void addEntry(Entry e) {
        entries.add(e);
        if (e.isInternalEntry()) {
            e.getChild().setParent(this);
        }
        updateMBR(e.getMBR());
    }

    /**
     * Αφαιρεί ένα entry από τον κόμβο.
     */
    public void removeEntry(Entry e) {
        entries.remove(e);
        recomputeMBRUpward();
    }

    /**
     * Επιστρέφει αν ο κόμβος έχει πλέον λιγότερα entries από το κατώτερο όριο.
     * (Χρησιμοποιείται κατά τη διαγραφή.)
     */
    public boolean underflows(int m) {
        return entries.size() < m;
    }

    @Override
    public String toString() {
        return "Node{level=" + level + ", isLeaf=" + isLeaf +
                ", #entries=" + entries.size() + "}";
    }
}
