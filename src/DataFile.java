// DataFile.java
//
// Υλοποίηση DataFile με block size 32 KB (32768 bytes),
// append-only insert logic, και μεθόδους readRecord, insertRecord, κ.λπ.
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class DataFile {
    public static final int BLOCK_SIZE = 32 * 1024; // 32 KB ανά block
    private final int dimension;
    private final int slotsPerBlock;
    private final int recordSize;
    private final FileChannel channel;
    private final String filename;

    // Για append-only εισαγωγή
    private int currentBlockId;
    private int nextSlot;
    private int liveCount;

    /**
     * Δημιουργεί (ή ανοίγει) το αρχείο δεδομένων.
     * Αν δεν υπάρχει, το φτιάχνει και γράφει header=0 στο πρώτο block.
     *
     * @param filename Το όνομα αρχείου (π.χ. "map.dbf").
     * @param dim      Η διάσταση (π.χ. 2).
     * @throws IOException σε περίπτωση I/O σφάλματος.
     */
    public DataFile(String filename, int dim) throws IOException {
        this.filename = filename;
        this.dimension = dim;
        // Κάθε record: 8 bytes για id (long) + 256 bytes για όνομα + 8×dim bytes για coords
        this.recordSize = 8 + 256 + 8 * dim;
        // Κάθε block: 4 bytes header + slots
        this.slotsPerBlock = (BLOCK_SIZE - 4) / recordSize;

        File f = new File(filename);
        if (!f.exists()) {
            // Δημιουργούμε νέο αρχείο: header=0 στο πρώτο block
            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            raf.setLength(0);
            this.channel = raf.getChannel();

            ByteBuffer header = ByteBuffer.allocate(4);
            header.putInt(0).flip();
            channel.write(header, 0L);

            this.currentBlockId = 0;
            this.nextSlot = 0;
            this.liveCount = 0;
        } else {
            // Άνοιγμα υπάρχοντος αρχείου
            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            this.channel = raf.getChannel();

            long fileSize = channel.size();
            if (fileSize == 0) {
                // Κενό αρχείο: αντιμετώπιση ως νέο
                this.currentBlockId = 0;
                this.nextSlot = 0;
                this.liveCount = 0;
                ByteBuffer header = ByteBuffer.allocate(4);
                header.putInt(0).flip();
                channel.write(header, 0L);
            } else {
                int totalBlocks = (int) (fileSize / BLOCK_SIZE);
                this.currentBlockId = totalBlocks - 1;

                long offsetHeader = (long) currentBlockId * BLOCK_SIZE;
                ByteBuffer buf = ByteBuffer.allocate(4);
                channel.read(buf, offsetHeader);
                buf.flip();
                this.liveCount = buf.getInt();
                this.nextSlot = liveCount;
                if (nextSlot >= slotsPerBlock) {
                    createNewBlock();
                }
            }
        }
    }

    /** Επιστρέφει το όνομα αρχείου. */
    public String getFileName() {
        return filename;
    }

    /** Επιστρέφει τη διάσταση (π.χ. 2). */
    public int getDimension() {
        return dimension;
    }

    /** Επιστρέφει πόσα slots χωράει κάθε block. */
    public int getSlotsPerBlock() {
        return slotsPerBlock;
    }

    /** Επιστρέφει το μέγεθος κάθε εγγραφής (σε bytes). */
    public int getRecordSize() {
        return recordSize;
    }

    /** Επιστρέφει το FileChannel, χρήσιμο για απευθείας disk-reads. */
    public FileChannel getChannel() {
        return channel;
    }

    /**
     * Εισάγει το δοσμένο Record στο επόμενο διαθέσιμο slot (append-only).
     * Επιστρέφει έναν RecordPointer(blockId, slotId).
     *
     * @param r Το Record που θέλουμε να γράψουμε.
     * @return  RecordPointer δηλωτικό όπου καταχωρήθηκε.
     * @throws IOException σε περίπτωση I/O σφάλματος.
     */
    public RecordPointer insertRecord(Record r) throws IOException {
        if (nextSlot >= slotsPerBlock) {
            createNewBlock();
        }
        long blockOffset = (long) currentBlockId * BLOCK_SIZE;
        long slotOffset = blockOffset + 4L + (long) nextSlot * recordSize;

        ByteBuffer buf = ByteBuffer.allocate(recordSize);
        buf.putLong(r.getId());

        byte[] nameBytes = new byte[256];
        byte[] actualName = r.getName().getBytes(StandardCharsets.UTF_8);
        int len = Math.min(actualName.length, 256);
        System.arraycopy(actualName, 0, nameBytes, 0, len);
        buf.put(nameBytes);

        for (int i = 0; i < dimension; i++) {
            buf.putDouble(r.getCoords()[i]);
        }
        buf.flip();
        channel.write(buf, slotOffset);

        liveCount++;
        nextSlot++;

        // Ενημερώνουμε header στη θέση blockOffset
        ByteBuffer headerBuf = ByteBuffer.allocate(4);
        headerBuf.putInt(liveCount).flip();
        channel.write(headerBuf, blockOffset);

        return new RecordPointer(currentBlockId, nextSlot - 1);
    }

    /**
     * Διαβάζει από το δίσκο ένα record με βάση τον RecordPointer.
     * Επιστρέφει ένα αντικείμενο Record με id, name, coords.
     *
     * @param rp O RecordPointer (blockId, slotId).
     * @return   Record με τα πεδία id, name και coords.
     * @throws IOException σε περίπτωση I/O σφάλματος.
     */
    public Record readRecord(RecordPointer rp) throws IOException {
        int blockId = rp.getBlockId();
        int slotId = rp.getSlotId();
        long blockOffset = (long) blockId * BLOCK_SIZE;
        long slotOffset = blockOffset + 4L + (long) slotId * recordSize;

        ByteBuffer buf = ByteBuffer.allocate(recordSize);
        channel.read(buf, slotOffset);
        buf.flip();

        long id = buf.getLong();

        byte[] nameBytes = new byte[256];
        buf.get(nameBytes);
        String name = new String(nameBytes, "UTF-8").trim();

        double[] coords = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            coords[i] = buf.getDouble();
        }

        return new Record(id, name, coords);
    }

    /**
     * Δημιουργεί νέο block γράφοντας header=0 και μηδενίζοντας τους μετρητές.
     */
    private void createNewBlock() throws IOException {
        int newBlock = currentBlockId + 1;
        long blockOffset = (long) newBlock * BLOCK_SIZE;
        ByteBuffer headerBuf = ByteBuffer.allocate(4);
        headerBuf.putInt(0).flip();
        channel.write(headerBuf, blockOffset);

        this.currentBlockId = newBlock;
        this.nextSlot = 0;
        this.liveCount = 0;
    }

    /** Κλείνει το FileChannel. */
    public void close() throws IOException {
        channel.close();
    }
}
