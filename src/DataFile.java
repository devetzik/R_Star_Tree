// DataFile.java
//
// DataFile με block-size 32 KB, όπου το block 0 χρησιμοποιείται μόνον για metadata.
// Τα δεδομένα (records) ξεκινούν από το block 1 και μετά.

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class DataFile {
    public static final int BLOCK_SIZE = 32 * 1024; // 32 KB ανά block

    private final int dimension;        // διάσταση των συντεταγμένων (π.χ. 2)
    private final int recordSize;       // 8 (id) + 256 (name) + 8×dim (coords)
    private final int slotsPerBlock;    // (BLOCK_SIZE - 4 bytes header) / recordSize
    private final FileChannel channel;
    private final String filename;

    // Για append-only εισαγωγή
    private int currentBlockId; // το τρέχον data-block στο οποίο γράφουμε (ξεκινάει από 1)
    private int nextSlot;       // θέση (slot index) μέσα στο currentBlockId (0..slotsPerBlock-1)
    private int totalRecords;   // συνολικό πλήθος εγγραφών
    private int totalBlocks;    // συνολικό πλήθος blocks (συμπεριλαμβανομένου του block 0)

    /**
     * Δημιουργεί (ή ανοίγει) το αρχείο δεδομένων.
     * Το block 0 κρατάει metadata, τα data blocks ξεκινούν από block 1.
     *
     * @param filename Το όνομα αρχείου (π.χ. "map.dbf").
     * @param dim      Η διάσταση (π.χ. 2).
     * @throws IOException σε περίπτωση I/O σφάλματος.
     */
    public DataFile(String filename, int dim) throws IOException {
        this.filename = filename;
        this.dimension = dim;
        // Κάθε record: 8 bytes id + 256 bytes name + 8×dim bytes coords
        this.recordSize = 8 + 256 + 8 * dim;
        // Σε κάθε data-block, header = 4 bytes (liveCount), και μετά τα record slots
        this.slotsPerBlock = (BLOCK_SIZE - 4) / recordSize;

        File f = new File(filename);
        if (!f.exists()) {
            // Δημιουργία νέου αρχείου, φτιάχνουμε το block 0 με metadata
            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            raf.setLength(0);
            this.channel = raf.getChannel();

            // Αρχικά: totalRecords = 0, totalBlocks = 1 (μόνο block 0 υπάρχει)
            this.totalRecords = 0;
            this.totalBlocks = 1;

            // Γράφουμε metadata στο block 0:
            //   offset 0..3: totalRecords
            //   offset 4..7: totalBlocks
            ByteBuffer metaBuf = ByteBuffer.allocate(BLOCK_SIZE);
            metaBuf.putInt(0); // totalRecords
            metaBuf.putInt(1); // totalBlocks
            metaBuf.flip();
            channel.write(metaBuf, 0L);

            // Το πρώτο data-block είναι το block 1
            this.currentBlockId = 1;
            this.nextSlot = 0;
        } else {
            // Άνοιγμα υπάρχοντος αρχείου
            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            this.channel = raf.getChannel();

            long fileSize = channel.size();
            if (fileSize < BLOCK_SIZE) {
                // Έστω ότι το αρχείο είναι μικρότερο από ένα block → ξεκινάμε από την αρχή
                this.totalRecords = 0;
                this.totalBlocks = 1;
                ByteBuffer metaBuf = ByteBuffer.allocate(BLOCK_SIZE);
                metaBuf.putInt(0);
                metaBuf.putInt(1);
                metaBuf.flip();
                channel.write(metaBuf, 0L);

                this.currentBlockId = 1;
                this.nextSlot = 0;
            } else {
                // Διαβάζουμε metadata από block 0 (offset 0):
                ByteBuffer headerBuf = ByteBuffer.allocate(8);
                channel.read(headerBuf, 0L);
                headerBuf.flip();
                this.totalRecords = headerBuf.getInt();
                this.totalBlocks = headerBuf.getInt();

                // Τελευταίο data-block είναι το blockId = totalBlocks - 1
                this.currentBlockId = totalBlocks - 1;
                long lastBlockOffset = (long) currentBlockId * BLOCK_SIZE;

                // Διαβάζουμε το liveCount (πόσα slots έχουν γραφεί) στο header του τελευταίου block
                ByteBuffer liveBuf = ByteBuffer.allocate(4);
                channel.read(liveBuf, lastBlockOffset);
                liveBuf.flip();
                int liveCountInLast = liveBuf.getInt();

                if (liveCountInLast < slotsPerBlock) {
                    // Υπάρχουν ακόμα ελεύθερα slots στο τελευταίο data-block
                    this.nextSlot = liveCountInLast;
                } else {
                    // Το τελευταίο data-block είναι γεμάτο → δημιουργούμε νέο
                    this.currentBlockId = totalBlocks; // νέο blockId
                    this.totalBlocks++;                // αυξάνουμε τις συνολικές σελίδες
                    this.nextSlot = 0;
                    writeMetadata();                   // ενημέρωση block 0
                }
            }
        }
    }

    /** @return Πόσα slots (records) χωρούν σε κάθε data-block. */
    public int getSlotsPerBlock() {
        return slotsPerBlock;
    }

    /** @return Μέγεθος κάθε εγγραφής (bytes). */
    public int getRecordSize() {
        return recordSize;
    }

    /** @return Η διάσταση (π.χ. 2). */
    public int getDimension() {
        return dimension;
    }

    /** @return Το underlying FileChannel (για range/kNN/skyline). */
    public FileChannel getChannel() {
        return channel;
    }

    /** Ενημερώνει τα πεδία totalRecords, totalBlocks στο block 0. */
    private void writeMetadata() throws IOException {
        ByteBuffer metaBuf = ByteBuffer.allocate(BLOCK_SIZE);
        metaBuf.putInt(totalRecords);
        metaBuf.putInt(totalBlocks);
        metaBuf.flip();
        channel.write(metaBuf, 0L);
    }

    /**
     * Εισαγωγή ενός νέου record στο αρχείο.
     * Επιστρέφει RecordPointer (blockId ≥ 1, slotId).
     */
    public RecordPointer insertRecord(Record rec) throws IOException {
        long blockOffset = (long) currentBlockId * BLOCK_SIZE;
        // Αν το currentBlockId δεν υπάρχει ακόμη στο αρχείο, το δημιουργούμε
        if (blockOffset + BLOCK_SIZE > channel.size()) {
            // Νέο data-block: γράφουμε header live=0 στο ξεκίνημά του
            ByteBuffer liveBuf = ByteBuffer.allocate(4);
            liveBuf.putInt(0).flip();
            channel.write(liveBuf, blockOffset);
        }

        // Θέση του current record: offset = blockOffset + 4 (bytes header) + slotId * recordSize
        long slotPos = blockOffset + 4L + (long) nextSlot * recordSize;

        // (α) Γράφουμε id (8 bytes)
        ByteBuffer idBuf = ByteBuffer.allocate(8);
        idBuf.putLong(rec.getId()).flip();
        channel.write(idBuf, slotPos);

        // (β) Γράφουμε name (256 bytes, UTF-8 padded με μηδενικά)
        ByteBuffer nameBuf = ByteBuffer.allocate(256);
        byte[] nameBytes = rec.getName().getBytes(StandardCharsets.UTF_8);
        int copyLen = Math.min(nameBytes.length, 256);
        nameBuf.put(nameBytes, 0, copyLen);
        nameBuf.flip();
        channel.write(nameBuf, slotPos + 8);

        // (γ) Γράφουμε coords (8×dim bytes)
        ByteBuffer coordsBuf = ByteBuffer.allocate(8 * dimension);
        for (double c : rec.getCoords()) {
            coordsBuf.putDouble(c);
        }
        coordsBuf.flip();
        channel.write(coordsBuf, slotPos + 8 + 256);

        // Ενημερώνουμε το live count αυτού του block (header)
        int liveInBlock = nextSlot + 1;
        ByteBuffer liveBuf = ByteBuffer.allocate(4);
        liveBuf.putInt(liveInBlock).flip();
        channel.write(liveBuf, blockOffset);

        // Αυξάνουμε counters
        RecordPointer rp = new RecordPointer(currentBlockId, nextSlot);
        nextSlot++;
        totalRecords++;

        // Αν γεμίσαμε το block, ανοίγουμε νέο data-block στο επόμενο insert
        if (nextSlot >= slotsPerBlock) {
            currentBlockId = totalBlocks; // νέο blockId
            totalBlocks++;
            nextSlot = 0;
            writeMetadata();             // ενημέρωση metadata (block 0)
        }

        return rp;
    }

    /**
     * Διαβάζει ένα Record με βάση το RecordPointer (blockId, slotId).
     * Επιστρέφει το αντικείμενο Record.
     */
    public Record readRecord(RecordPointer rp) throws IOException {
        long blockOffset = (long) rp.getBlockId() * BLOCK_SIZE;

        // (α) Διαβάζουμε id (8 bytes)
        long slotPos = blockOffset + 4L + (long) rp.getSlotId() * recordSize;
        ByteBuffer idBuf = ByteBuffer.allocate(8);
        channel.read(idBuf, slotPos);
        idBuf.flip();
        long id = idBuf.getLong();

        // (β) Διαβάζουμε name (256 bytes)
        ByteBuffer nameBuf = ByteBuffer.allocate(256);
        channel.read(nameBuf, slotPos + 8);
        nameBuf.flip();
        int strLen = 0;
        while (strLen < 256 && nameBuf.get(strLen) != 0) {
            strLen++;
        }
        byte[] nameBytes = new byte[strLen];
        nameBuf.position(0);
        nameBuf.get(nameBytes, 0, strLen);
        String name = new String(nameBytes, StandardCharsets.UTF_8);

        // (γ) Διαβάζουμε coords (8×dim bytes)
        ByteBuffer coordsBuf = ByteBuffer.allocate(8 * dimension);
        channel.read(coordsBuf, slotPos + 8 + 256);
        coordsBuf.flip();
        double[] coords = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            coords[i] = coordsBuf.getDouble();
        }

        return new Record(id, name, coords);
    }

    /** Κλείνει το underlying FileChannel. */
    public void close() throws IOException {
        channel.close();
    }
}
