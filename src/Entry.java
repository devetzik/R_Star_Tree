// Entry.java
//
// Μια εγγραφή (entry) σε κόμβο του R*-tree. Κάθε Entry έχει ένα MBR και είτε
// δείχνει σε ένα RecordPointer (leaf), είτε σε ένα παιδικό Node (internal).

public class Entry {
    private final MBR mbr;
    private final RecordPointer ptr;  // αν != null, τότε leaf-entry
    private final Node child;         // αν != null, τότε internal-entry

    /** Κατασκευαστής για leaf-entry (σημείο). */
    public Entry(MBR mbr, RecordPointer ptr) {
        this.mbr = mbr.clone();
        this.ptr = ptr;
        this.child = null;
    }

    /** Κατασκευαστής για internal-entry (υποδέντρο). */
    public Entry(MBR mbr, Node child) {
        this.mbr = mbr.clone();
        this.child = child;
        this.ptr = null;
    }

    public MBR getMBR() {
        return mbr.clone();
    }

    public RecordPointer getPointer() {
        return ptr;
    }

    public Node getChild() {
        return child;
    }

    public boolean isLeafEntry() {
        return ptr != null;
    }

    public boolean isInternalEntry() {
        return child != null;
    }

    @Override
    public String toString() {
        if (isLeafEntry()) {
            return "Entry{MBR=" + mbr + ", ptr=" + ptr + "}";
        } else {
            return "Entry{MBR=" + mbr + ", childNodeLevel=" + child.getLevel() + "}";
        }
    }
}
