import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;

public class IndexFile {
    public static final int BLOCK_SIZE_INDEX = 32 * 1024; // 32KB σελίδα
    private final int DIM;               // διάσταση (π.χ. 2)
    public final int M = 50;             // μέγιστες εγγραφές ανά κόμβο
    private final int RECORD_SIZE_INDEX; // bytes για κάθε Node
    private final int SLOTS_PER_PAGE;    // slots/page (συνήθως 1)

    private FileChannel channel;

    public IndexFile(String filename, int dimension) throws IOException {
        this.DIM = dimension;
        // 1B isLeaf + 4B level + 4B parentPage
        int headerBytes = 1 + 4 + 4;
        // Κάθε εγγραφή (entry): 16·DIM bytes (ΜΒR) + 8 bytes (pointer)
        int entryBytes = (16 * DIM) + 8;
        this.RECORD_SIZE_INDEX = headerBytes + M * entryBytes;
        this.SLOTS_PER_PAGE = (BLOCK_SIZE_INDEX - 4) / RECORD_SIZE_INDEX;

        Path path = Paths.get(filename);
        channel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    /**
     * Γράφει/ενημερώνει τον κόμβο node σε συγκεκριμένο pageId.
     * Αν pageId<0, δημιουργεί καινούργια σελίδα στο τέλος.
     * Επιστρέφει το pageId όπου γράφτηκε.
     */
    public int writeNode(int pageId, Node node) throws IOException {
        boolean isLeaf = node.isLeaf();
        int level = node.getLevel();
        int parentPage = node.getParentPage();
        ByteBuffer buf = ByteBuffer.allocate(BLOCK_SIZE_INDEX);
        // Header: liveCount = 1 (έγκυρος)
        buf.putInt(1);
        // 1B isLeaf
        buf.put((byte) (isLeaf ? 1 : 0));
        // 4B level
        buf.putInt(level);
        // 4B parentPage
        buf.putInt(parentPage);

        int writtenEntries = node.getEntries().size();
        for (int i = 0; i < M; i++) {
            if (i < writtenEntries) {
                Entry e = node.getEntries().get(i);
                double[] mn = e.getMBR().getMin();
                double[] mx = e.getMBR().getMax();
                // 16·DIM bytes για min+max
                for (int d = 0; d < DIM; d++) buf.putDouble(mn[d]);
                for (int d = 0; d < DIM; d++) buf.putDouble(mx[d]);
                if (isLeaf) {
                    // Leaf: recordPointer = 8 bytes (blockId, slotId)
                    RecordPointer rp = e.getPointer();
                    buf.putInt(rp.getBlockId());
                    buf.putInt(rp.getSlotId());
                } else {
                    // Internal: childPage (4B) + padding (4B)
                    int childPage = e.getChildPage();
                    buf.putInt(childPage);
                    buf.putInt(0); // padding
                }
            } else {
                // Γεμίζουμε με zeros
                for (int b = 0; b < (16 * DIM) + 8; b++) {
                    buf.put((byte) 0);
                }
            }
        }

        // Γεμίζουμε υπόλοιπο σελίδας με μηδενικά
        while (buf.position() < BLOCK_SIZE_INDEX) {
            buf.put((byte) 0);
        }
        buf.flip();

        if (pageId < 0) {
            long fileSize = channel.size();
            pageId = (int) (fileSize / BLOCK_SIZE_INDEX);
        }

        long offset = (long) pageId * BLOCK_SIZE_INDEX;
        channel.write(buf, offset);
        channel.force(true);

        return pageId;
    }

    /**
     * Διαβάζει και επιστρέφει τον κόμβο που βρίσκεται στο pageId.
     * Τα στοιχεία των παιδιών (childPage) αποθηκεύονται στο Entry,
     * αλλά δεν έχουν συνδεθεί ακόμα ως αντικείμενα Node.
     */
    public Node readNode(int pageId) throws IOException {
        long offset = (long) pageId * BLOCK_SIZE_INDEX;
        ByteBuffer buf = ByteBuffer.allocate(BLOCK_SIZE_INDEX);
        channel.read(buf, offset);
        buf.flip();

        int liveCount = buf.getInt();
        if (liveCount <= 0) return null;

        boolean isLeaf = buf.get() == 1;
        int level = buf.getInt();
        int parentPage = buf.getInt();

        Node node = new Node(level, isLeaf);
        node.setPageId(pageId);
        node.setParentPage(parentPage);

        for (int i = 0; i < M; i++) {
            double[] mn = new double[DIM];
            double[] mx = new double[DIM];
            for (int d = 0; d < DIM; d++) mn[d] = buf.getDouble();
            for (int d = 0; d < DIM; d++) mx[d] = buf.getDouble();
            MBR mbr = new MBR(mn, mx);

            int a = buf.getInt();
            int b = buf.getInt();
            if (isLeaf) {
                if (a != 0 || b != 0) {
                    RecordPointer rp = new RecordPointer(a, b);
                    node.addEntry(new Entry(mbr, rp));
                }
            } else {
                if (a != 0) {
                    node.addEntry(new Entry(mbr, a));
                }
            }
        }
        node.recomputeMBR();
        return node;
    }

    public void close() throws IOException {
        channel.close();
    }
}
