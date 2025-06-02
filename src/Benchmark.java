// Benchmark.java
//
// Μέτρηση χρόνων κατασκευής index (R*-tree) με δύο τεχνικές (ένα-προς-ένα vs bulkLoad)
// και εκτέλεση ερωτημάτων περιοχής (range), k-NN και skyline, τόσο στο R*-tree όσο και “σειριακά”
// στο DataFile (brute‐force scan). Όλοι οι χρόνοι εκτυπώνονται σε milliseconds.

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class Benchmark {
    // Παράμετροι
    private static final String OSM_FILE       = "map.osm";
    private static final String DATAFILE_NAME  = "map.dbf";
    private static final int    DIMENSIONS     = 2;         // lat / lon
    private static final int    K_NEIGHBORS    = 5;         // για k-NN
    private static final int    NUM_RANGE_QUERIES   = 10;    // πόσα τυχαία range queries
    private static final int    NUM_KNN_QUERIES     = 10;    // πόσα τυχαία k-NN queries
    private static final int    NUM_POINTS_FOR_QUERIES = 100; // θα πάρουμε 100 τυχαία points από το dataset

    public static void main(String[] args) {
        try {
            // 1) Φόρτωση όλων των κόμβων από το OSM σε List<Record>
            System.out.println("1) Φόρτωση OSM κόμβων σε ενδιάμεση λίστα Record...");
            long t0 = System.nanoTime();
            List<Record> records = loadAllOSMRecords(OSM_FILE);
            long t1 = System.nanoTime();
            System.out.printf("   Φορτώθηκαν %d κόμβοι σε %.2f ms%n",
                    records.size(), (t1 - t0) / 1_000_000.0);

            // 2) Δημιουργία DataFile — θα το χρησιμοποιήσουμε επίσης για brute‐force
            System.out.println("\n2) Δημιουργία κενών DataFile + R*-tree...");
            DataFile dfEmpty = new DataFile(DATAFILE_NAME, DIMENSIONS);
            RStarTree treeEmpty = new RStarTree(DIMENSIONS, dfEmpty);
            dfEmpty.close(); // απλά για να δημιουργηθεί το αρχείο
            System.out.println("   DataFile και R*-tree δημιουργήθηκαν κενά.");

            // 3) Κατασκευή R*-tree με εισαγωγή ένα-προς-ένα
            System.out.println("\n3) Κατασκευή R*-tree με insert ένα-προς-ένα...");
            DataFile df1 = new DataFile(DATAFILE_NAME, DIMENSIONS);
            RStarTree tree1 = new RStarTree(DIMENSIONS, df1);
            long tInsertStart = System.nanoTime();
            for (Record r : records) {
                tree1.insert(r);
            }
            long tInsertEnd = System.nanoTime();
            System.out.printf("   Εισαγωγή %d εγγραφών ένα-προς-ένα σε R*-tree: %.2f ms%n",
                    records.size(), (tInsertEnd - tInsertStart) / 1_000_000.0);
            df1.close();

            // 4) Κατασκευή R*-tree με bulkLoad
            System.out.println("\n4) Κατασκευή R*-tree με bulkLoad...");
            DataFile df2 = new DataFile(DATAFILE_NAME, DIMENSIONS);
            RStarTree tree2 = new RStarTree(DIMENSIONS, df2);
            long tBulkStart = System.nanoTime();
            tree2.bulkLoad(records);
            long tBulkEnd = System.nanoTime();
            System.out.printf("   BulkLoad %d εγγραφών σε R*-tree: %.2f ms%n",
                    records.size(), (tBulkEnd - tBulkStart) / 1_000_000.0);
            df2.close();

            // 5) Προετοιμασία sample points από τα records
            System.out.println("\n5) Προετοιμασία τυχαίων ερωτημάτων...");
            List<double[]> samplePoints = pickRandomCoordinates(records, NUM_POINTS_FOR_QUERIES);

            // 6) Εκτέλεση Range Queries
            System.out.println("\n6) Ερωτήματα περιοχής (Range Queries):");
            // 6.1) Σειριακό (brute-force scan)
            DataFile dfSerialRange = new DataFile(DATAFILE_NAME, DIMENSIONS);
            RStarTree treeForRange2 = new RStarTree(DIMENSIONS, dfSerialRange);
            treeForRange2.bulkLoad(records); // χρειάζεται index για το δεύτερο μέτρο

            double totalSerialRangeTime = 0.0;
            double totalIndexRangeTime  = 0.0;
            for (int i = 0; i < NUM_RANGE_QUERIES; i++) {
                double[] center = samplePoints.get(i);
                double radius = 0.01; // ±0.01 σε κάθε διάσταση
                double[] minR = { center[0] - radius, center[1] - radius };
                double[] maxR = { center[0] + radius, center[1] + radius };

                // --- Serial scan στο DataFile
                long ts0 = System.nanoTime();
                List<RecordPointer> serialRes = rangeQuerySerial(dfSerialRange, minR, maxR);
                long ts1 = System.nanoTime();
                double deltaSerial = (ts1 - ts0) / 1_000_000.0;
                totalSerialRangeTime += deltaSerial;

                // --- R*-tree index (χρησιμοποιούμε treeForRange2)
                long ti0 = System.nanoTime();
                List<RecordPointer> indexRes = treeForRange2.rangeQuery(minR, maxR);
                long ti1 = System.nanoTime();
                double deltaIndex = (ti1 - ti0) / 1_000_000.0;
                totalIndexRangeTime += deltaIndex;
            }
            System.out.printf("   Μέσος χρόνος RangeQuery (σειριακό): %.2f ms%n",
                    totalSerialRangeTime / NUM_RANGE_QUERIES);
            System.out.printf("   Μέσος χρόνος RangeQuery (R*-tree): %.2f ms%n",
                    totalIndexRangeTime / NUM_RANGE_QUERIES);
            dfSerialRange.close();

            // 7) Εκτέλεση k-NN Queries
            System.out.println("\n7) Ερωτήματα k-NN:");
            DataFile dfSerialKNN = new DataFile(DATAFILE_NAME, DIMENSIONS);
            RStarTree treeForKNN = new RStarTree(DIMENSIONS, dfSerialKNN);
            treeForKNN.bulkLoad(records);

            double totalSerialKnnTime = 0.0;
            double totalIndexKnnTime  = 0.0;
            for (int i = 0; i < NUM_KNN_QUERIES; i++) {
                double[] queryPt = samplePoints.get(i);

                // --- Serial k-NN (brute-force)
                long tsn0 = System.nanoTime();
                List<RecordPointer> serialKnnRes = kNNQuerySerial(dfSerialKNN, queryPt, K_NEIGHBORS);
                long tsn1 = System.nanoTime();
                double deltaSerialKnn = (tsn1 - tsn0) / 1_000_000.0;
                totalSerialKnnTime += deltaSerialKnn;

                // --- R*-tree index k-NN
                long tin0 = System.nanoTime();
                List<RecordPointer> indexKnnRes = treeForKNN.kNNQuery(queryPt, K_NEIGHBORS);
                long tin1 = System.nanoTime();
                double deltaIndexKnn = (tin1 - tin0) / 1_000_000.0;
                totalIndexKnnTime += deltaIndexKnn;
            }
            System.out.printf("   Μέσος χρόνος k-NN (σειριακό): %.2f ms%n",
                    totalSerialKnnTime / NUM_KNN_QUERIES);
            System.out.printf("   Μέσος χρόνος k-NN (R*-tree): %.2f ms%n",
                    totalIndexKnnTime / NUM_KNN_QUERIES);
            dfSerialKNN.close();

            // 8) Εκτέλεση Skyline Query
            System.out.println("\n8) Ερώτημα Skyline:");
            DataFile dfSerialSky = new DataFile(DATAFILE_NAME, DIMENSIONS);
            RStarTree treeForSky = new RStarTree(DIMENSIONS, dfSerialSky);
            treeForSky.bulkLoad(records);

            // --- Serial Skyline (brute-force)
            long tss0 = System.nanoTime();
            List<RecordPointer> serialSkyRes = skylineSerial(dfSerialSky);
            long tss1 = System.nanoTime();
            double deltaSerialSky = (tss1 - tss0) / 1_000_000.0;

            // --- Index‐based Skyline
            long tix0 = System.nanoTime();
            List<RecordPointer> indexSkyRes = treeForSky.skylineQuery();
            long tix1 = System.nanoTime();
            double deltaIndexSky = (tix1 - tix0) / 1_000_000.0;

            System.out.printf("   Χρόνος Skyline (σειριακό): %.2f ms%n", deltaSerialSky);
            System.out.printf("   Χρόνος Skyline (R*-tree): %.2f ms%n", deltaIndexSky);
            dfSerialSky.close();

            System.out.println("\n== Ολοκλήρωση Benchmark ==");

        } catch (IOException ioe) {
            ioe.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Βοηθητικές Μέθοδοι (με χρήση των getters του DataFile)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Διαβάζει όλους τους κόμβους από το OSM αρχείο σε List<Record>.
     * Χρησιμοποιεί ένα custom SAX handler για να συλλέξει μόνο τα Record.
     */
    private static List<Record> loadAllOSMRecords(String osmFilename) throws Exception {
        List<Record> out = new ArrayList<>();

        javax.xml.parsers.SAXParserFactory factory = javax.xml.parsers.SAXParserFactory.newInstance();
        javax.xml.parsers.SAXParser saxParser = factory.newSAXParser();

        saxParser.parse(new java.io.File(osmFilename), new org.xml.sax.helpers.DefaultHandler() {
            private boolean inNode = false;
            private long currentId;
            private double currentLat;
            private double currentLon;
            private String currentName;

            @Override
            public void startElement(String uri, String localName, String qName, org.xml.sax.Attributes attributes) {
                if (qName.equals("node")) {
                    inNode = true;
                    currentName = "";
                    currentId = Long.parseLong(attributes.getValue("id"));
                    currentLat = Double.parseDouble(attributes.getValue("lat"));
                    currentLon = Double.parseDouble(attributes.getValue("lon"));
                } else if (inNode && qName.equals("tag")) {
                    String k = attributes.getValue("k");
                    if (k != null && k.equals("name")) {
                        currentName = attributes.getValue("v");
                    }
                }
            }

            @Override
            public void endElement(String uri, String localName, String qName) {
                if (qName.equals("node") && inNode) {
                    double[] coords = { currentLat, currentLon };
                    out.add(new Record(currentId, currentName == null ? "" : currentName, coords));
                    inNode = false;
                }
            }
        });

        return out;
    }

    /**
     * Brute‐force range query σε όλο το DataFile.
     */
    private static List<RecordPointer> rangeQuerySerial(DataFile df,
                                                        double[] minCoords, double[] maxCoords) throws IOException {
        List<RecordPointer> result = new ArrayList<>();

        // Καθορισμός παραμέτρων από τα getters
        int blockSize = DataFile.BLOCK_SIZE;
        long fileSize = new java.io.RandomAccessFile(DATAFILE_NAME, "r").length();
        int totalBlocks = (int) (fileSize / blockSize);
        int dim = df.getDimension();
        java.nio.channels.FileChannel channel = df.getChannel();
        int slotsPerBlock = df.getSlotsPerBlock();
        int recordSize = df.getRecordSize();

        for (int blkId = 0; blkId < totalBlocks; blkId++) {
            long blockOffset = (long) blkId * blockSize;
            java.nio.ByteBuffer headerBuf = java.nio.ByteBuffer.allocate(4);
            channel.read(headerBuf, blockOffset);
            headerBuf.flip();
            int live = headerBuf.getInt();

            for (int slot = 0; slot < slotsPerBlock; slot++) {
                long slotPos = blockOffset + 4L + (long) slot * recordSize;
                java.nio.ByteBuffer idBuf = java.nio.ByteBuffer.allocate(8);
                channel.read(idBuf, slotPos);
                idBuf.flip();
                long id = idBuf.getLong();
                if (id == -1L) continue; // διαγραμμένο

                // Διαβάζουμε coords
                long coordsPos = slotPos + 8 + 256;
                java.nio.ByteBuffer coordsBuf = java.nio.ByteBuffer.allocate(8 * dim);
                channel.read(coordsBuf, coordsPos);
                coordsBuf.flip();
                double[] coords = new double[dim];
                for (int i = 0; i < dim; i++) {
                    coords[i] = coordsBuf.getDouble();
                }
                boolean inside = true;
                for (int i = 0; i < dim; i++) {
                    if (coords[i] < minCoords[i] || coords[i] > maxCoords[i]) {
                        inside = false;
                        break;
                    }
                }
                if (inside) {
                    result.add(new RecordPointer(blkId, slot));
                }
            }
        }
        return result;
    }

    /**
     * Brute‐force k‐NN query: διαβάζουμε όλα τα σημεία και κρατάμε τα k με τη μικρότερη απόσταση.
     */
    private static List<RecordPointer> kNNQuerySerial(DataFile df,
                                                      double[] queryPt, int k) throws IOException {
        class Pair {
            double dist;
            RecordPointer rp;
            Pair(double d, RecordPointer rp) { this.dist = d; this.rp = rp; }
        }
        List<Pair> distList = new ArrayList<>();

        int blockSize = DataFile.BLOCK_SIZE;
        long fileSize = new java.io.RandomAccessFile(DATAFILE_NAME, "r").length();
        int totalBlocks = (int) (fileSize / blockSize);
        int dim = df.getDimension();
        java.nio.channels.FileChannel channel = df.getChannel();
        int slotsPerBlock = df.getSlotsPerBlock();
        int recordSize = df.getRecordSize();

        for (int blkId = 0; blkId < totalBlocks; blkId++) {
            long blockOffset = (long) blkId * blockSize;
            java.nio.ByteBuffer headerBuf = java.nio.ByteBuffer.allocate(4);
            channel.read(headerBuf, blockOffset);
            headerBuf.flip();
            int live = headerBuf.getInt();

            for (int slot = 0; slot < slotsPerBlock; slot++) {
                long slotPos = blockOffset + 4L + (long) slot * recordSize;
                java.nio.ByteBuffer idBuf = java.nio.ByteBuffer.allocate(8);
                channel.read(idBuf, slotPos);
                idBuf.flip();
                long id = idBuf.getLong();
                if (id == -1L) continue;

                long coordsPos = slotPos + 8 + 256;
                java.nio.ByteBuffer coordsBuf = java.nio.ByteBuffer.allocate(8 * dim);
                channel.read(coordsBuf, coordsPos);
                coordsBuf.flip();
                double[] coords = new double[dim];
                for (int i = 0; i < dim; i++) {
                    coords[i] = coordsBuf.getDouble();
                }
                double dist2 = 0;
                for (int i = 0; i < dim; i++) {
                    double diff = coords[i] - queryPt[i];
                    dist2 += diff * diff;
                }
                distList.add(new Pair(Math.sqrt(dist2), new RecordPointer(blkId, slot)));
            }
        }

        distList.sort(Comparator.comparingDouble(p -> p.dist));
        List<RecordPointer> result = new ArrayList<>();
        for (int i = 0; i < Math.min(k, distList.size()); i++) {
            result.add(distList.get(i).rp);
        }
        return result;
    }

    /**
     * Brute‐force Skyline: διαβάζουμε όλα τα σημεία, τα αποθηκεύουμε σε λίστα και κάνουμε διπλό loop.
     */
    private static List<RecordPointer> skylineSerial(DataFile df) throws IOException {
        class PointWithRP {
            double[] coords;
            RecordPointer rp;
        }
        List<PointWithRP> points = new ArrayList<>();

        int blockSize = DataFile.BLOCK_SIZE;
        long fileSize = new java.io.RandomAccessFile(DATAFILE_NAME, "r").length();
        int totalBlocks = (int) (fileSize / blockSize);
        int dim = df.getDimension();
        java.nio.channels.FileChannel channel = df.getChannel();
        int slotsPerBlock = df.getSlotsPerBlock();
        int recordSize = df.getRecordSize();

        for (int blkId = 0; blkId < totalBlocks; blkId++) {
            long blockOffset = (long) blkId * blockSize;
            java.nio.ByteBuffer headerBuf = java.nio.ByteBuffer.allocate(4);
            channel.read(headerBuf, blockOffset);
            headerBuf.flip();
            int live = headerBuf.getInt();

            for (int slot = 0; slot < slotsPerBlock; slot++) {
                long slotPos = blockOffset + 4L + (long) slot * recordSize;
                java.nio.ByteBuffer idBuf = java.nio.ByteBuffer.allocate(8);
                channel.read(idBuf, slotPos);
                idBuf.flip();
                long id = idBuf.getLong();
                if (id == -1L) continue;

                long coordsPos = slotPos + 8 + 256;
                java.nio.ByteBuffer coordsBuf = java.nio.ByteBuffer.allocate(8 * dim);
                channel.read(coordsBuf, coordsPos);
                coordsBuf.flip();
                double[] coords = new double[dim];
                for (int i = 0; i < dim; i++) {
                    coords[i] = coordsBuf.getDouble();
                }
                PointWithRP p = new PointWithRP();
                p.coords = coords;
                p.rp = new RecordPointer(blkId, slot);
                points.add(p);
            }
        }

        List<RecordPointer> skyline = new ArrayList<>();
        for (int i = 0; i < points.size(); i++) {
            double[] p = points.get(i).coords;
            boolean dominated = false;
            for (int j = 0; j < points.size(); j++) {
                if (i == j) continue;
                double[] q = points.get(j).coords;
                if (dominates(q, p)) {
                    dominated = true;
                    break;
                }
            }
            if (!dominated) {
                skyline.add(points.get(i).rp);
            }
        }
        return skyline;
    }

    private static boolean dominates(double[] q, double[] p) {
        boolean strictlyBetter = false;
        for (int i = 0; i < q.length; i++) {
            if (q[i] > p[i]) return false;
            if (q[i] < p[i]) strictlyBetter = true;
        }
        return strictlyBetter;
    }

    /**
     * Επιλέγει τυχαία NUM σημείων από τη λίστα records και επιστρέφει τις συντεταγμένες τους.
     */
    private static List<double[]> pickRandomCoordinates(List<Record> records, int num) {
        List<double[]> out = new ArrayList<>();
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        int n = records.size();
        for (int i = 0; i < num; i++) {
            Record r = records.get(rnd.nextInt(n));
            out.add(r.getCoords());
        }
        return out;
    }
}
