// Node.java
//
// Κλάση που αναπαριστά έναν κόμβο στο R*-tree.
// Κάθε κόμβος έχει level (0=leaf), isLeaf flag, λίστα Entry, MBR, parentPage, pageId.
import java.util.ArrayList;
import java.util.List;

public class Node {
    private int level;
    private boolean isLeaf;
    private List<Entry> entries;
    private MBR mbr;
    private int parentPage = -1;
    private int pageId = -1;

    public Node(int level, boolean isLeaf) {
        this.level = level;
        this.isLeaf = isLeaf;
        this.entries = new ArrayList<>();
        // Εδώ δεν ορίζουμε mbr ακόμη (θα υπολογιστεί όταν προστεθεί το πρώτο entry)
        this.mbr = null;
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

    public int getParentPage() {
        return parentPage;
    }

    public void setParentPage(int parentPage) {
        this.parentPage = parentPage;
    }

    public int getPageId() {
        return pageId;
    }

    public void setPageId(int pageId) {
        this.pageId = pageId;
    }

    public MBR getMBR() {
        return mbr;
    }

    public void addEntry(Entry e) {
        entries.add(e);
        if (mbr == null) {
            mbr = e.getMBR().clone();
        } else {
            mbr = MBR.union(mbr, e.getMBR());
        }
    }

    public void removeEntry(Entry e) {
        entries.remove(e);
        recomputeMBR();
    }

    public void recomputeMBR() {
        if (entries.isEmpty()) {
            mbr = null;
            return;
        }
        MBR newMBR = entries.get(0).getMBR().clone();
        for (int i = 1; i < entries.size(); i++) {
            newMBR = MBR.union(newMBR, entries.get(i).getMBR());
        }
        mbr = newMBR;
    }

    public void recomputeMBRUpward() {
        recomputeMBR();
        // Ο γονέας θα ενημερωθεί από το RStarTree όταν χρειάζεται
    }

    public boolean underflows(int m) {
        return entries.size() < m;
    }
}
