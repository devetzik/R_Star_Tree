// RecordPointer.java
//
// Απλός wrapper που κρατάει τη θέση μιας εγγραφής μέσα σε έναν DataFile block.
// Χρησιμοποιείται μέσα στον R*-tree για να γνωρίζουμε πού βρίσκεται η εγγραφή στο δίσκο.
public class RecordPointer {
    private final int blockId;
    private final int slotId;

    public RecordPointer(int blockId, int slotId) {
        this.blockId = blockId;
        this.slotId = slotId;
    }

    public int getBlockId() {
        return blockId;
    }

    public int getSlotId() {
        return slotId;
    }

    @Override
    public String toString() {
        return "RecordPointer{blockId=" + blockId + ", slotId=" + slotId + '}';
    }
}
