import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;


public class Benchmark {
    private static final String OSM_FILE               = "map.osm";
    private static final String DATAFILE_NAME          = "map.dbf";
    private static final String INDEXFILE_NAME         = "index.idx";
    private static final int    DIMENSIONS             = 2;
    private static final int    K_NEIGHBORS            = 5;
    private static final int    NUM_RANGE_QUERIES      = 10;
    private static final int    NUM_KNN_QUERIES        = 10;
    private static final int    NUM_POINTS_FOR_QUERIES = 100;

    public static void main(String[] args) {
        try {
            // ───────────────────────────────────────────────────────────────
            // 1) Φόρτωση όλων των κόμβων από το OSM σε λίστα Record
            // ───────────────────────────────────────────────────────────────
            System.out.println("1) Φόρτωση OSM κόμβων σε ενδιάμεση λίστα Record...");
            long t0 = System.nanoTime();
            List<Record> records = loadAllOSMRecords(OSM_FILE);
            long t1 = System.nanoTime();
            System.out.printf("   Φορτώθηκαν %d κόμβοι σε %.2f ms%n",
                    records.size(), (t1 - t0) / 1_000_000.0);

            // ───────────────────────────────────────────────────────────────
            // 2) Γράφουμε **μία φορά** όλα τα records στο DataFile
            //    (σειριακή εισαγωγή), αποθηκεύουμε (RecordPointer, coords)
            //    σε δύο λίστες, ώστε στο index build να μην ξανασκανάρουμε
            //    τον δίσκο για το DataFile.
            // ───────────────────────────────────────────────────────────────
            System.out.println("\n2) Μία φορά σειριακή εισαγωγή όλων των Record στο DataFile...");
            DataFile dfInit = new DataFile(DATAFILE_NAME, DIMENSIONS);

            List<RecordPointer> allPointers = new ArrayList<>(records.size());
            List<double[]>      allCoords   = new ArrayList<>(records.size());
            long tDFStart = System.nanoTime();
            for (Record r : records) {
                RecordPointer rp = dfInit.insertRecord(r);
                allPointers.add(rp);
                allCoords.add(r.getCoords());
            }
            long tDFEnd = System.nanoTime();
            System.out.printf("   Ολοκληρώθηκε DataFile insert: %.2f ms%n",
                    (tDFEnd - tDFStart) / 1_000_000.0);

            dfInit.close();

            // ───────────────────────────────────────────────────────────────
            // 3) Κατασκευή R*-tree με “insert ένα-προς-ένα” (με insertPointer)
            // ───────────────────────────────────────────────────────────────
            System.out.println("\n3) Κατασκευή R*-tree με insertPointer (ένα-προς-ένα)...");
            DataFile df1 = new DataFile(DATAFILE_NAME, DIMENSIONS);
            IndexFile idx1 = new IndexFile(INDEXFILE_NAME, DIMENSIONS);
            RStarTree tree1 = new RStarTree(DIMENSIONS, df1, idx1);

            long tInsertStart = System.nanoTime();
            for (int i = 0; i < allPointers.size(); i++) {
                tree1.insertPointer(allPointers.get(i), allCoords.get(i));
            }
            long tInsertEnd = System.nanoTime();
            System.out.printf("   Ολοκληρώθηκε insertPointer ένα-προς-ένα: %.2f ms%n",
                    (tInsertEnd - tInsertStart) / 1_000_000.0);

            df1.close();
            idx1.close();

            // ───────────────────────────────────────────────────────────────
            // 4) Κατασκευή R*-tree με bulkLoad
            // ───────────────────────────────────────────────────────────────
            System.out.println("\n4) Κατασκευή R*-tree με bulkLoad...");
            DataFile df2 = new DataFile(DATAFILE_NAME, DIMENSIONS);
            IndexFile idx2 = new IndexFile(INDEXFILE_NAME, DIMENSIONS);
            RStarTree tree2 = new RStarTree(DIMENSIONS, df2, idx2);

            long tBulkStart = System.nanoTime();
            tree2.bulkLoad(records);
            long tBulkEnd = System.nanoTime();
            System.out.printf("   BulkLoad %d εγγραφών σε R*-tree: %.2f ms%n",
                    records.size(), (tBulkEnd - tBulkStart) / 1_000_000.0);

            df2.close();
            idx2.close();

            // ───────────────────────────────────────────────────────────────
            // 5) Προετοιμασία τυχαίων σημείων για queries
            // ───────────────────────────────────────────────────────────────
            System.out.println("\n5) Προετοιμασία τυχαίων ερωτημάτων...");
            List<double[]> samplePoints = pickRandomCoordinates(records, NUM_POINTS_FOR_QUERIES);

            // ───────────────────────────────────────────────────────────────
            // 6) Εκτέλεση Range Queries
            // ───────────────────────────────────────────────────────────────
            System.out.println("\n6) Ερωτήματα περιοχής (Range Queries):");
            DataFile dfSerialRange = new DataFile(DATAFILE_NAME, DIMENSIONS);
            IndexFile idxForRange = new IndexFile(INDEXFILE_NAME, DIMENSIONS);
            RStarTree treeForRange = new RStarTree(DIMENSIONS, dfSerialRange, idxForRange);
            treeForRange.bulkLoad(records);

            double totalSerialRangeTime = 0.0;
            double totalIndexRangeTime  = 0.0;
            for (int i = 0; i < NUM_RANGE_QUERIES; i++) {
                double[] center = samplePoints.get(i);
                double radius = 0.01;
                double[] minR = { center[0] - radius, center[1] - radius };
                double[] maxR = { center[0] + radius, center[1] + radius };

                // 6.1) Σειριακό brute‐force scan
                long ts0 = System.nanoTime();
                List<RecordPointer> serialRes = rangeQuerySerial(dfSerialRange, minR, maxR);
                long ts1 = System.nanoTime();
                totalSerialRangeTime += (ts1 - ts0) / 1_000_000.0;

                // 6.2) R*-tree index
                long ti0 = System.nanoTime();
                List<RecordPointer> indexRes = treeForRange.rangeQuery(minR, maxR);
                long ti1 = System.nanoTime();
                totalIndexRangeTime += (ti1 - ti0) / 1_000_000.0;
            }
            System.out.printf("   Μέσος χρόνος RangeQuery (σειριακό scan): %.2f ms%n",
                    totalSerialRangeTime / NUM_RANGE_QUERIES);
            System.out.printf("   Μέσος χρόνος RangeQuery (R*-tree): %.2f ms%n",
                    totalIndexRangeTime / NUM_RANGE_QUERIES);
            dfSerialRange.close();
            idxForRange.close();

            // ───────────────────────────────────────────────────────────────
            // 7) Εκτέλεση k-NN Queries
            // ───────────────────────────────────────────────────────────────
            System.out.println("\n7) Ερωτήματα k-NN:");
            DataFile dfSerialKNN = new DataFile(DATAFILE_NAME, DIMENSIONS);
            IndexFile idxForKNN = new IndexFile(INDEXFILE_NAME, DIMENSIONS);
            RStarTree treeForKNN = new RStarTree(DIMENSIONS, dfSerialKNN, idxForKNN);
            treeForKNN.bulkLoad(records);

            double totalSerialKnnTime = 0.0;
            double totalIndexKnnTime  = 0.0;
            for (int i = 0; i < NUM_KNN_QUERIES; i++) {
                double[] queryPt = samplePoints.get(i);

                // 7.1) Σειριακό k-NN (brute‐force)
                long tsn0 = System.nanoTime();
                List<RecordPointer> serialKnnRes = kNNQuerySerial(dfSerialKNN, queryPt, K_NEIGHBORS);
                long tsn1 = System.nanoTime();
                totalSerialKnnTime += (tsn1 - tsn0) / 1_000_000.0;

                // 7.2) R*-tree k-NN
                long tin0 = System.nanoTime();
                List<RecordPointer> indexKnnRes = treeForKNN.kNNQuery(queryPt, K_NEIGHBORS);
                long tin1 = System.nanoTime();
                totalIndexKnnTime += (tin1 - tin0) / 1_000_000.0;
            }
            System.out.printf("   Μέσος χρόνος k-NN (σειριακό scan): %.2f ms%n",
                    totalSerialKnnTime / NUM_KNN_QUERIES);
            System.out.printf("   Μέσος χρόνος k-NN (R*-tree): %.2f ms%n",
                    totalIndexKnnTime / NUM_KNN_QUERIES);
            dfSerialKNN.close();
            idxForKNN.close();

            // ───────────────────────────────────────────────────────────────
            // 8) Εκτέλεση Skyline Query
            // ───────────────────────────────────────────────────────────────
            System.out.println("\n8) Ερώτημα Skyline:");
            DataFile dfSerialSky = new DataFile(DATAFILE_NAME, DIMENSIONS);
            IndexFile idxForSky = new IndexFile(INDEXFILE_NAME, DIMENSIONS);
            RStarTree treeForSky = new RStarTree(DIMENSIONS, dfSerialSky, idxForSky);
            treeForSky.bulkLoad(records);

            // 8.1) Σειριακό Skyline (brute‐force)
            long tss0 = System.nanoTime();
            List<RecordPointer> serialSkyRes = skylineSerial(dfSerialSky);
            long tss1 = System.nanoTime();
            double deltaSerialSky = (tss1 - tss0) / 1_000_000.0;

            // 8.2) R*-tree Skyline
            long tix0 = System.nanoTime();
            List<RecordPointer> indexSkyRes = treeForSky.skylineQuery();
            long tix1 = System.nanoTime();
            double deltaIndexSky = (tix1 - tix0) / 1_000_000.0;

            System.out.printf("   Χρόνος Skyline (σειριακό): %.2f ms%n", deltaSerialSky);
            System.out.printf("   Χρόνος Skyline (R*-tree): %.2f ms%n", deltaIndexSky);
            dfSerialSky.close();
            idxForSky.close();

            System.out.println("\n== Ολοκλήρωση Benchmark ==");
        }
        catch (IOException | ParserConfigurationException | SAXException e) {
            e.printStackTrace();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Βοηθητικές Μέθοδοι
    // ─────────────────────────────────────────────────────────────────────────

    private static List<Record> loadAllOSMRecords(String osmFilename)
            throws ParserConfigurationException, SAXException, IOException {
        List<Record> out = new ArrayList<>();
        javax.xml.parsers.SAXParserFactory factory =
                javax.xml.parsers.SAXParserFactory.newInstance();
        javax.xml.parsers.SAXParser saxParser = factory.newSAXParser();

        saxParser.parse(new java.io.File(osmFilename),
                new org.xml.sax.helpers.DefaultHandler() {
                    private boolean inNode = false;
                    private long currentId;
                    private double currentLat;
                    private double currentLon;
                    private String currentName;

                    @Override
                    public void startElement(String uri, String localName,
                                             String qName, org.xml.sax.Attributes attributes) {
                        if (qName.equals("node")) {
                            inNode = true;
                            currentName = "";
                            currentId = Long.parseLong(attributes.getValue("id"));
                            currentLat = Double.parseDouble(attributes.getValue("lat"));
                            currentLon = Double.parseDouble(attributes.getValue("lon"));
                        } else if (inNode && qName.equals("tag")) {
                            String k = attributes.getValue("k");
                            if ("name".equals(k)) {
                                currentName = attributes.getValue("v");
                            }
                        }
                    }

                    @Override
                    public void endElement(String uri, String localName, String qName) {
                        if (qName.equals("node") && inNode) {
                            double[] coords = {currentLat, currentLon};
                            out.add(new Record(currentId,
                                    currentName == null ? "" : currentName,
                                    coords));
                            inNode = false;
                        }
                    }
                });
        return out;
    }

    private static List<RecordPointer> rangeQuerySerial(DataFile df,
                                                        double[] minCoords,
                                                        double[] maxCoords) throws IOException {
        List<RecordPointer> result = new ArrayList<>();
        FileChannel channel = df.getChannel();
        int blockSize    = DataFile.BLOCK_SIZE;
        long fileSize    = channel.size();
        int totalBlocks  = (int) (fileSize / blockSize);
        int dim          = df.getDimension();
        int slotsPerBlock= df.getSlotsPerBlock();
        int recordSize   = df.getRecordSize();

        // Ξεκινάμε από το block 1 (το block 0 είναι metadata)
        for (int blkId = 1; blkId < totalBlocks; blkId++) {
            long blockOffset = (long) blkId * blockSize;
            // Διαβάζουμε το live count από τα πρώτα 4 bytes
            ByteBuffer headerBuf = ByteBuffer.allocate(4);
            channel.read(headerBuf, blockOffset);
            headerBuf.flip();
            int live = headerBuf.getInt();

            for (int slot = 0; slot < live; slot++) {
                long slotPos = blockOffset + 4L + (long) slot * recordSize;
                ByteBuffer idBuf = ByteBuffer.allocate(8);
                channel.read(idBuf, slotPos);
                idBuf.flip();
                long id = idBuf.getLong();
                if (id == -1L) continue; // διαγραμμένο (ή άκυρο)

                long coordsPos = slotPos + 8 + 256;
                ByteBuffer coordsBuf = ByteBuffer.allocate(8 * dim);
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


    private static List<RecordPointer> kNNQuerySerial(DataFile df,
                                                      double[] queryPt,
                                                      int k) throws IOException {
        class Pair {
            double dist;
            RecordPointer rp;
            Pair(double d, RecordPointer rp) {
                this.dist = d;
                this.rp   = rp;
            }
        }
        List<Pair> distList = new ArrayList<>();

        FileChannel channel = df.getChannel();
        int blockSize    = DataFile.BLOCK_SIZE;
        long fileSize    = channel.size();
        int totalBlocks  = (int) (fileSize / blockSize);
        int dim          = df.getDimension();
        int slotsPerBlock= df.getSlotsPerBlock();
        int recordSize   = df.getRecordSize();

        for (int blkId = 1; blkId < totalBlocks; blkId++) {
            long blockOffset = (long) blkId * blockSize;
            // Διαβάζουμε live count
            ByteBuffer headerBuf = ByteBuffer.allocate(4);
            channel.read(headerBuf, blockOffset);
            headerBuf.flip();
            int live = headerBuf.getInt();

            for (int slot = 0; slot < live; slot++) {
                long slotPos = blockOffset + 4L + (long) slot * recordSize;
                ByteBuffer idBuf = ByteBuffer.allocate(8);
                channel.read(idBuf, slotPos);
                idBuf.flip();
                long id = idBuf.getLong();
                if (id == -1L) continue;

                long coordsPos = slotPos + 8 + 256;
                ByteBuffer coordsBuf = ByteBuffer.allocate(8 * dim);
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
                distList.add(new Pair(Math.sqrt(dist2),
                        new RecordPointer(blkId, slot)));
            }
        }

        distList.sort(Comparator.comparingDouble(p -> p.dist));
        List<RecordPointer> result = new ArrayList<>();
        for (int i = 0; i < Math.min(k, distList.size()); i++) {
            result.add(distList.get(i).rp);
        }
        return result;
    }


    private static List<RecordPointer> skylineSerial(DataFile df) throws IOException {
        class PointWithRP {
            double[] coords;
            RecordPointer rp;
        }
        List<PointWithRP> points = new ArrayList<>();

        FileChannel channel = df.getChannel();
        int blockSize    = DataFile.BLOCK_SIZE;
        long fileSize    = channel.size();
        int totalBlocks  = (int) (fileSize / blockSize);
        int dim          = df.getDimension();
        int slotsPerBlock= df.getSlotsPerBlock();
        int recordSize   = df.getRecordSize();

        for (int blkId = 1; blkId < totalBlocks; blkId++) {
            long blockOffset = (long) blkId * blockSize;
            ByteBuffer headerBuf = ByteBuffer.allocate(4);
            channel.read(headerBuf, blockOffset);
            headerBuf.flip();
            int live = headerBuf.getInt();

            for (int slot = 0; slot < live; slot++) {
                long slotPos = blockOffset + 4L + (long) slot * recordSize;
                ByteBuffer idBuf = ByteBuffer.allocate(8);
                channel.read(idBuf, slotPos);
                idBuf.flip();
                long id = idBuf.getLong();
                if (id == -1L) continue;

                long coordsPos = slotPos + 8 + 256;
                ByteBuffer coordsBuf = ByteBuffer.allocate(8 * dim);
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
