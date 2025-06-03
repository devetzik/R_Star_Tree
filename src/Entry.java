// Entry.java
//
// Αναπαριστά μία καταχώρηση στον Node: είτε leaf-entry (pointer σε Record) είτε internal-entry (childPage).

public class Entry {
    private MBR mbr;
    private RecordPointer pointer; // leaf-entry (αν non-null)
    private int childPage;         // internal-entry (αν >=0)

    // Leaf constructor
    public Entry(MBR mbr, RecordPointer rp) {
        this.mbr = mbr;
        this.pointer = rp;
        this.childPage = -1;
    }

    // Internal constructor
    public Entry(MBR mbr, int childPage) {
        this.mbr = mbr;
        this.childPage = childPage;
        this.pointer = null;
    }

    public boolean isLeafEntry() {
        return pointer != null;
    }

    public boolean isInternalEntry() {
        return childPage >= 0;
    }

    public RecordPointer getPointer() {
        return pointer;
    }

    public int getChildPage() {
        return childPage;
    }

    public MBR getMBR() {
        return mbr;
    }

    public void setMBR(MBR m) {
        this.mbr = m;
    }
}
