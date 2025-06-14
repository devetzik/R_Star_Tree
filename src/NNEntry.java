public class NNEntry implements Comparable<NNEntry> {
    private final Node node;      // αν πρόκειται για κόμβο
    private final Entry entry;    // αν πρόκειται για leaf entry
    private final double distance;

    // Constructor για node
    public NNEntry(Node node, double distance) {
        this.node = node;
        this.entry = null;
        this.distance = distance;
    }

    // Constructor για entry
    public NNEntry(Entry entry, double distance) {
        this.entry = entry;
        this.node = null;
        this.distance = distance;
    }

    public boolean isNode() {
        return node != null;
    }

    public Node getNode() {
        return node;
    }

    public Entry getEntry() {
        return entry;
    }

    @Override
    public int compareTo(NNEntry other) {
        return Double.compare(this.distance, other.distance);
    }
}
