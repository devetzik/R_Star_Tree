// NNEntry.java
//
// Ένα «wrapper» που χρησιμοποιείται στον k-NN αλγόριθμο Best-First (PriorityQueue).
// Περιέχει ή έναν κόμβο (isNode=true) ή ένα Entry (leaf result) με τη σχετική
// απόσταση dist.

public class NNEntry implements Comparable<NNEntry> {
    private final boolean isNode;
    private final Node node;      // αν isNode=true
    private final Entry entry;    // αν isNode=false
    private final double dist;    // η απόσταση από το query point

    /** Κατασκευαστής για κόμβο. */
    public NNEntry(Node node, double dist) {
        this.isNode = true;
        this.node = node;
        this.entry = null;
        this.dist = dist;
    }

    /** Κατασκευαστής για leaf-entry. */
    public NNEntry(Entry entry, double dist) {
        this.isNode = false;
        this.node = null;
        this.entry = entry;
        this.dist = dist;
    }

    public boolean isNode() {
        return isNode;
    }

    public Node getNode() {
        return node;
    }

    public Entry getEntry() {
        return entry;
    }

    public double getDist() {
        return dist;
    }

    @Override
    public int compareTo(NNEntry o) {
        return Double.compare(this.dist, o.dist);
    }
}
