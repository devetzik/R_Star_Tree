// DataFile.java
//
// Δισκο-βασισμένο αρχείο για αποθήκευση Record. Κάθε block = 32KB,
// με header 4 bytes για τα ζωντανά slots, έπειτα slots σταθερού μήκους.

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;

public class DataFile {
    public static final int BLOCK_SIZE = 32 * 1024; // 32 KB ανά block
    private final int dimension;
    private final int NAME_LENGTH = 256; // bytes
    public final int RECORD_SIZE;       // bytes ανά εγγραφή
    private final int SLOTS_PER_BLOCK;  // πόσα slots χωράει ένα block

    private FileChannel channel;
    private String filename;

    public DataFile(String filename, int dimension) throws IOException {
        this.filename = filename;
        this.dimension = dimension;
        // RECORD_SIZE = 8 bytes (long id) + 256 bytes (name) + 8*dimension bytes (double coords)
        this.RECORD_SIZE = 8 + NAME_LENGTH + 8 * dimension;
        // Σε κάθε block, 4 bytes header + slots
        this.SLOTS_PER_BLOCK = (BLOCK_SIZE - 4) / RECORD_SIZE;

        Path path = Paths.get(filename);
        channel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    /**
     * Εισαγωγή ενός Record στο DataFile.
     * Επιστρέφει RecordPointer(blockId, slotId).
     */
    public RecordPointer insertRecord(Record rec) throws IOException {
        // Βρίσκουμε το πρώτο block με διαθέσιμο slot
        long fileSize = channel.size();
        int totalBlocks = (int) (fileSize / BLOCK_SIZE);
        if (fileSize % BLOCK_SIZE != 0) {
            totalBlocks += 1;
        }
        for (int blkId = 0; blkId < totalBlocks; blkId++) {
            long blockOffset = (long) blkId * BLOCK_SIZE;
            ByteBuffer headerBuf = ByteBuffer.allocate(4);
            channel.read(headerBuf, blockOffset);
            headerBuf.flip();
            int live = headerBuf.getInt();
            if (live < SLOTS_PER_BLOCK) {
                // Βρίσκουμε το πρώτο ελεύθερο slot εντός του block
                for (int slot = 0; slot < SLOTS_PER_BLOCK; slot++) {
                    long slotPos = blockOffset + 4L + (long) slot * RECORD_SIZE;
                    ByteBuffer idBuf = ByteBuffer.allocate(8);
                    channel.read(idBuf, slotPos);
                    idBuf.flip();
                    long existingId = idBuf.getLong();
                    if (existingId == -1L) {
                        // Δωρεάν slot
                        // Γράφουμε το id
                        ByteBuffer writeBuf = ByteBuffer.allocate(RECORD_SIZE);
                        writeBuf.putLong(rec.getId());
                        // Γράφουμε το name (256 bytes, λεπτώς γεμίζουμε με μηδενικά)
                        byte[] nameBytes = rec.getName().getBytes("UTF-8");
                        if (nameBytes.length > NAME_LENGTH) {
                            writeBuf.put(nameBytes, 0, NAME_LENGTH);
                        } else {
                            writeBuf.put(nameBytes);
                            for (int i = nameBytes.length; i < NAME_LENGTH; i++) {
                                writeBuf.put((byte) 0);
                            }
                        }
                        // Γράφουμε coords
                        double[] coords = rec.getCoords();
                        for (int i = 0; i < dimension; i++) {
                            writeBuf.putDouble(coords[i]);
                        }
                        writeBuf.flip();
                        channel.write(writeBuf, slotPos);

                        // Ενημερώνουμε header (live+1)
                        ByteBuffer incHeader = ByteBuffer.allocate(4);
                        incHeader.putInt(live + 1);
                        incHeader.flip();
                        channel.write(incHeader, blockOffset);

                        return new RecordPointer(blkId, slot);
                    }
                }
            }
        }
        // Αν δεν υπήρχε block με ελεύθερο slot, φτιάχνουμε καινούργιο block
        int newBlockId = totalBlocks; // επόμενος διαθέσιμος
        // Γράφουμε header=1
        ByteBuffer headerBuf = ByteBuffer.allocate(4);
        headerBuf.putInt(1);
        headerBuf.flip();
        channel.write(headerBuf, (long) newBlockId * BLOCK_SIZE);

        // Κενά τα υπόλοιπα slots
        ByteBuffer zeroBuf = ByteBuffer.allocate(BLOCK_SIZE - 4);
        zeroBuf.put(new byte[BLOCK_SIZE - 4]);
        zeroBuf.flip();
        channel.write(zeroBuf, (long) newBlockId * BLOCK_SIZE + 4L);

        // Γράφουμε την εγγραφή στο πρώτο slot του καινούργιου block
        long slotPos = (long) newBlockId * BLOCK_SIZE + 4L;
        ByteBuffer writeBuf = ByteBuffer.allocate(RECORD_SIZE);
        writeBuf.putLong(rec.getId());
        byte[] nameBytes = rec.getName().getBytes("UTF-8");
        if (nameBytes.length > NAME_LENGTH) {
            writeBuf.put(nameBytes, 0, NAME_LENGTH);
        } else {
            writeBuf.put(nameBytes);
            for (int i = nameBytes.length; i < NAME_LENGTH; i++) {
                writeBuf.put((byte) 0);
            }
        }
        double[] coords = rec.getCoords();
        for (int i = 0; i < dimension; i++) {
            writeBuf.putDouble(coords[i]);
        }
        writeBuf.flip();
        channel.write(writeBuf, slotPos);

        return new RecordPointer(newBlockId, 0);
    }

    /**
     * Διαβάζει ένα Record από το DataFile, δεδομένο RecordPointer.
     */
    public Record readRecord(RecordPointer rp) throws IOException {
        int blkId = rp.getBlockId();
        int slotId = rp.getSlotId();
        long slotPos = (long) blkId * BLOCK_SIZE + 4L + (long) slotId * RECORD_SIZE;

        ByteBuffer buffer = ByteBuffer.allocate(RECORD_SIZE);
        channel.read(buffer, slotPos);
        buffer.flip();

        long id = buffer.getLong();
        byte[] nameBytes = new byte[NAME_LENGTH];
        buffer.get(nameBytes);
        String name = new String(nameBytes, "UTF-8").trim();

        double[] coords = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            coords[i] = buffer.getDouble();
        }
        return new Record(id, name, coords);
    }

    /**
     * Διαγράφει το Record που δείχνει το RecordPointer (βάζουμε id=-1 και live--).
     */
    public void deleteRecord(RecordPointer rp) throws IOException {
        int blkId = rp.getBlockId();
        int slotId = rp.getSlotId();
        long blockOffset = (long) blkId * BLOCK_SIZE;

        // Διαβάζουμε header
        ByteBuffer headerBuf = ByteBuffer.allocate(4);
        channel.read(headerBuf, blockOffset);
        headerBuf.flip();
        int live = headerBuf.getInt();

        // Γράφουμε id = -1 στο slot
        long slotPos = blockOffset + 4L + (long) slotId * RECORD_SIZE;
        ByteBuffer idBuf = ByteBuffer.allocate(8);
        idBuf.putLong(-1L);
        idBuf.flip();
        channel.write(idBuf, slotPos);

        // Ενημερώνουμε header = live - 1
        ByteBuffer decHeader = ByteBuffer.allocate(4);
        decHeader.putInt(live - 1);
        decHeader.flip();
        channel.write(decHeader, blockOffset);
    }

    public void close() throws IOException {
        channel.close();
    }

    /** Getters για Benchmarking/Brute‐Force access */
    public java.nio.channels.FileChannel getChannel() {
        return this.channel;
    }
    public int getSlotsPerBlock() {
        return this.SLOTS_PER_BLOCK;
    }
    public int getRecordSize() {
        return this.RECORD_SIZE;
    }
    public int getDimension() {
        return this.dimension;
    }
}
