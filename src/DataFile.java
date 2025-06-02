// DataFile.java
//
// Υλοποίηση αποθηκευτικού αρχείου (32 KB blocks) για Record:
//  - Κάθε block: 4 bytes header (αριθμός ζωντανών records) + (RECORD_SIZE * #slots).
//  - RECORD_SIZE = 8 bytes (id) + 256 bytes (όνομα) + 8*d bytes (συντεταγμένες).
//  - Όταν διαγράφουμε ένα record, απλώς βάζουμε το id = -1 και μειώνουμε το counter.

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

public class DataFile {
    public static final int BLOCK_SIZE = 32 * 1024; // 32 KB
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private final int dimension;
    private final int RECORD_SIZE;      // 8 bytes id + 256 bytes name + 8*d
    private final int SLOTS_PER_BLOCK;  // (BLOCK_SIZE - 4) / RECORD_SIZE

    /**
     * Δημιουργεί (ή ανοίγει) ένα αρχείο για εγγραφές d-διάστατων Record.
     * @param filename Το όνομα του αρχείου (π.χ. "datafile.dbf").
     * @param dimension Ο αριθμός διαστάσεων d για τις συντεταγμένες.
     */
    public DataFile(String filename, int dimension) throws IOException {
        this.dimension = dimension;
        this.RECORD_SIZE = 8 + 256 + 8 * dimension;
        this.SLOTS_PER_BLOCK = (BLOCK_SIZE - 4) / RECORD_SIZE;

        File f = new File(filename);
        boolean existed = f.exists();
        raf = new RandomAccessFile(f, "rw");
        channel = raf.getChannel();

        if (!existed) {
            // Δημιουργούμε το πρώτο block με header = 0
            raf.setLength(0);
            ByteBuffer buffer = ByteBuffer.allocate(BLOCK_SIZE);
            buffer.putInt(0);       // κανένα ζωντανό record
            buffer.position(BLOCK_SIZE);
            buffer.flip();
            channel.write(buffer, 0);
        }
    }

    /** Κλείνει το αρχείο. */
    public void close() throws IOException {
        channel.close();
        raf.close();
    }

    /**
     * Εισάγει ένα νέο Record στο αρχείο. Επιστρέφει RecordPointer.
     */
    public RecordPointer insertRecord(Record rec) throws IOException {
        long fileSize = raf.length();
        int totalBlocks = (int) (fileSize / BLOCK_SIZE);

        // 1) Ψάχνουμε ένα block με ελεύθετη θέση
        for (int blkId = 0; blkId < totalBlocks; blkId++) {
            long blockOffset = (long) blkId * BLOCK_SIZE;
            // Διαβάζουμε τον header (4 bytes = #ζωντανά records)
            ByteBuffer headerBuf = ByteBuffer.allocate(4);
            channel.read(headerBuf, blockOffset);
            headerBuf.flip();
            int live = headerBuf.getInt();

            if (live < SLOTS_PER_BLOCK) {
                // Υπάρχει σίγουρα κάποιο ελεύθερο slot (ή -1) μέσα σε αυτό το block
                for (int slot = 0; slot < SLOTS_PER_BLOCK; slot++) {
                    long slotPos = blockOffset + 4L + (long) slot * RECORD_SIZE;
                    // Διαβάζουμε το id για κάθε slot
                    ByteBuffer idBuf = ByteBuffer.allocate(8);
                    channel.read(idBuf, slotPos);
                    idBuf.flip();
                    long storedId = idBuf.getLong();

                    if (storedId == -1L) {
                        // Επαναχρησιμοποιούμε σκουπισμένο slot
                        writeRecordAt(slotPos, rec);
                        // Αύξηση live counter
                        ByteBuffer inc = ByteBuffer.allocate(4);
                        inc.putInt(live + 1);
                        inc.flip();
                        channel.write(inc, blockOffset);
                        return new RecordPointer(blkId, slot);
                    }
                }
                // Αν δεν βρέθηκε σκουπισμένο, χρησιμοποιούμε τον επόμενο ελεύθερο slot = live
                int slot = live;
                long slotPos = blockOffset + 4L + (long) slot * RECORD_SIZE;
                writeRecordAt(slotPos, rec);
                ByteBuffer inc = ByteBuffer.allocate(4);
                inc.putInt(live + 1);
                inc.flip();
                channel.write(inc, blockOffset);
                return new RecordPointer(blkId, slot);
            }
        }

        // 2) Όλα τα υπάρχοντα blocks είναι γεμάτα → δημιουργία νέου block
        int newBlkId = totalBlocks;
        long newBlockOffset = (long) newBlkId * BLOCK_SIZE;

        // Δημιουργούμε ένα ολοκαίνουριο block (με header = 1, καθώς εκεί μπαίνει το πρώτο record)
        ByteBuffer newBlock = ByteBuffer.allocate(BLOCK_SIZE);
        newBlock.putInt(1);
        // Γράφουμε το record στη θέση slot 0
        writeRecordAt(newBlock, 4L, rec);
        newBlock.position(BLOCK_SIZE);
        newBlock.flip();
        channel.write(newBlock, newBlockOffset);

        return new RecordPointer(newBlkId, 0);
    }

    /**
     * Διαβάζει ένα Record από τον δίσκο με βάση το RecordPointer.
     */
    public Record readRecord(RecordPointer rp) throws IOException {
        long blockOffset = (long) rp.getBlockId() * BLOCK_SIZE;
        long slotPos = blockOffset + 4L + (long) rp.getSlotId() * RECORD_SIZE;
        return readRecordAt(slotPos);
    }

    /**
     * Διαγράφει ένα Record (θέτει id = -1 και μειώνει τον counter).
     */
    public void deleteRecord(RecordPointer rp) throws IOException {
        long blockOffset = (long) rp.getBlockId() * BLOCK_SIZE;
        // Διαβάζουμε τον header
        ByteBuffer headerBuf = ByteBuffer.allocate(4);
        channel.read(headerBuf, blockOffset);
        headerBuf.flip();
        int live = headerBuf.getInt();
        if (live <= 0) return;

        // Θέτουμε id = -1
        long slotPos = blockOffset + 4L + (long) rp.getSlotId() * RECORD_SIZE;
        ByteBuffer negOne = ByteBuffer.allocate(8);
        negOne.putLong(-1L);
        negOne.flip();
        channel.write(negOne, slotPos);

        // Μειώνουμε τον header counter
        ByteBuffer dec = ByteBuffer.allocate(4);
        dec.putInt(live - 1);
        dec.flip();
        channel.write(dec, blockOffset);
    }

    /** Για debugging: εμφανίζει όλα τα records στο αρχείο. */
    public void dumpAll() throws IOException {
        long fileSize = raf.length();
        int totalBlocks = (int) (fileSize / BLOCK_SIZE);

        for (int blkId = 0; blkId < totalBlocks; blkId++) {
            long blockOffset = (long) blkId * BLOCK_SIZE;
            ByteBuffer headerBuf = ByteBuffer.allocate(4);
            channel.read(headerBuf, blockOffset);
            headerBuf.flip();
            int live = headerBuf.getInt();
            System.out.println("Block " + blkId + " έχει " + live + " εγγραφές:");

            for (int slot = 0; slot < SLOTS_PER_BLOCK; slot++) {
                long slotPos = blockOffset + 4L + (long) slot * RECORD_SIZE;
                ByteBuffer idBuf = ByteBuffer.allocate(8);
                channel.read(idBuf, slotPos);
                idBuf.flip();
                long storedId = idBuf.getLong();
                if (storedId != -1L) {
                    Record r = readRecordAt(slotPos);
                    System.out.print("  Slot " + slot + ": " + r);
                    System.out.println();
                }
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    //   Private helper methods
    // ─────────────────────────────────────────────────────────────────────────────

    /** Διαβάζει ένα record που είναι στη θέση slotPos. */
    private Record readRecordAt(long slotPos) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(RECORD_SIZE);
        channel.read(buf, slotPos);
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

    /** Γράφει ένα Record σε γνωστή θέση μέσα στο FileChannel. */
    private void writeRecordAt(long filePos, Record rec) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(RECORD_SIZE);
        buf.putLong(rec.getId());

        byte[] nameBytes = new byte[256];
        byte[] src = rec.getName().getBytes("UTF-8");
        int len = Math.min(src.length, 256);
        System.arraycopy(src, 0, nameBytes, 0, len);
        buf.put(nameBytes);

        double[] c = rec.getCoords();
        for (int i = 0; i < dimension; i++) {
            buf.putDouble(c[i]);
        }
        buf.flip();
        channel.write(buf, filePos);
    }

    /**
     * Ίδιο με πάνω, αλλά γράφει μέσα σε ByteBuffer που αντιπροσωπεύει ολόκληρο block.
     * Χρησιμοποιείται μόνο όταν φτιάχνουμε νέο block (στο insertRecord).
     */
    private void writeRecordAt(ByteBuffer blockBuf, long relativePos, Record rec) {
        blockBuf.position((int) relativePos);
        blockBuf.putLong(rec.getId());

        byte[] nameBytes = new byte[256];
        byte[] src = rec.getName().getBytes();
        int len = Math.min(src.length, 256);
        System.arraycopy(src, 0, nameBytes, 0, len);
        blockBuf.put(nameBytes);

        double[] c = rec.getCoords();
        for (int i = 0; i < dimension; i++) {
            blockBuf.putDouble(c[i]);
        }
    }

    /**
    * Επιστρέφει το FileChannel του DataFile (για low‐level διάβασμα block/slot).
     */
    public java.nio.channels.FileChannel getChannel() {
        return this.channel;
    }

    /**
     * Επιστρέφει πόσα slots χωράει κάθε block (SLOTS_PER_BLOCK).
     */
    public int getSlotsPerBlock() {
        return this.SLOTS_PER_BLOCK;
    }

    /**
     * Επιστρέφει το μέγεθος κάθε record (RECORD_SIZE).
     */
    public int getRecordSize() {
        return this.RECORD_SIZE;
    }

    /**
     * Επιστρέφει το πλήθος των διαστάσεων (dimension) που είχε καθοριστεί κατά το άνοιγμα.
     */
    public int getDimension() {
        return this.dimension;
    }
}
