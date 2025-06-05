// Δείκτης σε εγγραφή μέσα στο DataFile: blockId και slotId.

public class RecordPointer {
    private int blockId;
    private int slotId;

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
}
