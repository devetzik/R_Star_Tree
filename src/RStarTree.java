// RStarTree.java
//
// R*-tree χωρίς cache, με σωστή προώθηση του MBR προς τα πάνω μετά από κάθε εισαγωγή ή split.
// Προϋποθέτει ότι οι κλάσεις DataFile, IndexFile, Node, Entry, MBR, SplitResult, RecordPointer, Record, NNEntry
// βρίσκονται στο ίδιο πακέτο ή είναι import‐αρισμένες.
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class RStarTree {
    private final int DIM;
    private final int M = 50;   // Μέγιστος αριθμός entries ανά κόμβο
    private final int m = 25;   // Ελάχιστος αριθμός entries μετά split

    private final DataFile dataFile;
    private final IndexFile indexFile;

    private Node root;

    /**
     * Κατασκευαστής RStarTree.
     *
     * @param d   Διάσταση (π.χ. 2 για γεωχωρικά δεδομένα).
     * @param df  DataFile για εγγραφή/ανάγνωση records.
     * @param idx IndexFile για εγγραφή/ανάγνωση κόμβων.
     * @throws IOException σε περίπτωση I/O σφάλματος.
     */
    public RStarTree(int d, DataFile df, IndexFile idx) throws IOException {
        this.DIM = d;
        this.dataFile = df;
        this.indexFile = idx;

        // Δημιουργούμε νέο κενό root (leaf επίπεδο 0) και τον γράφουμε στο IndexFile
        Node newRoot = new Node(0, true);
        int rootPage = indexFile.writeNode(-1, newRoot);
        newRoot.setPageId(rootPage);
        newRoot.setParentPage(-1);
        this.root = newRoot;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Σειριακή εισαγωγή Record (insert).
    // ─────────────────────────────────────────────────────────────────────────
    /**
     * Εισάγει ένα Record: πρώτα στο DataFile και μετά ως leaf‐entry στο R*-tree.
     * Επιστρέφει τον RecordPointer για τη θέση στο DataFile.
     */
    public RecordPointer insert(Record rec) throws IOException {
        // 1) Εισαγωγή στο DataFile
        RecordPointer rp = dataFile.insertRecord(rec);

        // 2) Δημιουργία leaf‐entry
        MBR singleMBR = new MBR(rec.getCoords(), rec.getCoords());
        Entry newEntry = new Entry(singleMBR, rp);

        // 3) Επιλογή κατάλληλου φύλλου (chooseLeaf)
        Node leaf = chooseLeaf(root, newEntry);

        // 4) Προσθήκη στο leaf + γράφουμε πίσω + ενημέρωση MBR προς τα πάνω
        leaf.addEntry(newEntry);
        leaf.recomputeMBRUpward();
        indexFile.writeNode(leaf.getPageId(), leaf);
        adjustTree(leaf);

        // 5) Αν overflow, κάνουμε handleOverflow
        if (leaf.getEntries().size() > M) {
            handleOverflow(leaf);
        }

        // 6) Αν το root έχει αλλάξει, το ανανεώνουμε
        if (root.getParentPage() >= 0) {
            root = indexFile.readNode(root.getParentPage());
        }
        return rp;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Εισαγωγή pointer χωρίς DataFile (insertPointer).
    // ─────────────────────────────────────────────────────────────────────────
    /**
     * Εισάγει ένα Entry με δεδομένο RecordPointer και coords.
     * Δεν γράφει νέο Record στο DataFile—χρησιμοποιείται για το bulkLoad.
     */
    public void insertPointer(RecordPointer rp, double[] coords) throws IOException {
        MBR singleMBR = new MBR(coords, coords);
        Entry newEntry = new Entry(singleMBR, rp);

        Node leaf = chooseLeaf(root, newEntry);
        leaf.addEntry(newEntry);
        leaf.recomputeMBRUpward();
        indexFile.writeNode(leaf.getPageId(), leaf);
        adjustTree(leaf);

        if (leaf.getEntries().size() > M) {
            handleOverflow(leaf);
        }

        if (root.getParentPage() >= 0) {
            root = indexFile.readNode(root.getParentPage());
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Επιλογή κατάλληλου φύλλου (chooseLeaf).
    // ─────────────────────────────────────────────────────────────────────────
    private Node chooseLeaf(Node curr, Entry e) throws IOException {
        if (curr.isLeaf()) {
            return curr;
        }
        Entry best = null;
        double bestInc  = Double.POSITIVE_INFINITY;
        double bestArea = Double.POSITIVE_INFINITY;

        for (Entry c : curr.getEntries()) {
            if (!c.isInternalEntry()) continue;
            Node child = indexFile.readNode(c.getChildPage());
            // Βεβαιωνόμαστε ότι ο child έχει σωστό parentPage
            if (child.getParentPage() != curr.getPageId()) {
                child.setParentPage(curr.getPageId());
                indexFile.writeNode(child.getPageId(), child);
            }

            double inc  = c.getMBR().enlargement(e.getMBR());
            double area = c.getMBR().area();
            int childSz = child.getEntries().size();

            Node bestChildNode = (best == null
                    ? null
                    : indexFile.readNode(best.getChildPage()));
            int bestChildSz = (bestChildNode == null
                    ? Integer.MAX_VALUE
                    : bestChildNode.getEntries().size());

            if (inc < bestInc
                    || (inc == bestInc && area < bestArea)
                    || (inc == bestInc && area == bestArea && childSz < bestChildSz)) {
                best = c;
                bestInc = inc;
                bestArea = area;
            }
        }

        if (best == null) {
            throw new IllegalStateException(
                    "chooseLeaf: Δεν βρέθηκε internal entry σε κόμβο επιπέδου " + curr.getLevel());
        }
        return chooseLeaf(indexFile.readNode(best.getChildPage()), e);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Διαχείριση overflow: reinsert ή split (handleOverflow).
    // ─────────────────────────────────────────────────────────────────────────
    private void handleOverflow(Node N) throws IOException {
        if (N.getPageId() == root.getPageId()) {
            // Αν είναι root, απλώς split
            splitNode(N);
            return;
        }
        if (N.getLevel() > 0 && !hasBeenReinserted(N)) {
            reinsert(N);
        } else {
            splitNode(N);
        }
    }

    private boolean hasBeenReinserted(Node N) {
        // Αν θέλετε να αποφύγετε διπλή reinsert, βάλτε flag στο Node.
        return false;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Reinsert: αφαίρεση p entries και «συμπλήρωσή» τους από τη ρίζα
    // ─────────────────────────────────────────────────────────────────────────
    private void reinsert(Node N) throws IOException {
        int p = (int) Math.floor(0.3 * M);
        double[] centroid = new double[DIM];
        Arrays.fill(centroid, 0.0);

        // Υπολογισμός κέντρου βάρους όλων των centroids των entry‐MBR
        for (Entry e : N.getEntries()) {
            double[] mn = e.getMBR().getMin();
            double[] mx = e.getMBR().getMax();
            for (int i = 0; i < DIM; i++) {
                centroid[i] += (mn[i] + mx[i]) / 2.0;
            }
        }
        for (int i = 0; i < DIM; i++) {
            centroid[i] /= N.getEntries().size();
        }

        // Ταξινόμηση κατά απόσταση από centroid (πρώτα οι πιο απομακρυσμένες)
        List<Entry> sorted = new ArrayList<>(N.getEntries());
        sorted.sort((a, b) -> {
            double da = 0, db = 0;
            double[] ca = new double[DIM], cb = new double[DIM];
            for (int i = 0; i < DIM; i++) {
                ca[i] = (a.getMBR().getMin()[i] + a.getMBR().getMax()[i]) / 2.0;
                cb[i] = (b.getMBR().getMin()[i] + b.getMBR().getMax()[i]) / 2.0;
                da += Math.pow(ca[i] - centroid[i], 2);
                db += Math.pow(cb[i] - centroid[i], 2);
            }
            return Double.compare(db, da);
        });

        List<Entry> toReinsert = new ArrayList<>(sorted.subList(0, p));
        N.getEntries().removeAll(toReinsert);
        N.recomputeMBRUpward();
        indexFile.writeNode(N.getPageId(), N);
        adjustTree(N);

        // Επανεισάγουμε σταδιακά από τη ρίζα
        for (Entry e : toReinsert) {
            if (e.isLeafEntry()) {
                insertEntry(root, e, 0);
            } else {
                Node child = indexFile.readNode(e.getChildPage());
                insertEntry(root, e, child.getLevel());
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Bulk-load: bottom-up κατασκευή χωρίς πολλαπλά overflows
    // ─────────────────────────────────────────────────────────────────────────
    public void bulkLoad(List<Record> records) throws IOException {
        // Ταξινόμηση κατά πρώτη συντεταγμένη
        records.sort(Comparator.comparingDouble(r -> r.getCoords()[0]));

        // 1) Φτιάχνουμε όλα τα leaf entries (γραμμικά στο DataFile + MBR)
        List<Entry> leafEntries = new ArrayList<>();
        for (Record rec : records) {
            RecordPointer rp = dataFile.insertRecord(rec);
            MBR mbr = new MBR(rec.getCoords(), rec.getCoords());
            leafEntries.add(new Entry(mbr, rp));
        }

        // 2) Πακετάρουμε σε leaf nodes των M entries
        List<Node> leaves = new ArrayList<>();
        for (int i = 0; i < leafEntries.size(); i += M) {
            int end = Math.min(i + M, leafEntries.size());
            Node leaf = new Node(0, true);
            for (int j = i; j < end; j++) {
                leaf.addEntry(leafEntries.get(j));
            }
            leaf.recomputeMBRUpward();
            int pageLeaf = indexFile.writeNode(-1, leaf);
            leaf.setPageId(pageLeaf);
            leaf.setParentPage(-1);
            leaves.add(leaf);
        }

        // 3) Φτιάχνουμε τα εσωτερικά επίπεδα μέχρι τη ρίζα
        int level = 1;
        List<Node> currentLevel = leaves;
        while (currentLevel.size() > 1) {
            List<Node> nextLevel = new ArrayList<>();
            for (int i = 0; i < currentLevel.size(); i += M) {
                int end = Math.min(i + M, currentLevel.size());
                Node parent = new Node(level, false);
                for (int j = i; j < end; j++) {
                    Node child = currentLevel.get(j);
                    parent.addEntry(new Entry(child.getMBR(), child.getPageId()));
                }
                parent.recomputeMBRUpward();
                int pageParent = indexFile.writeNode(-1, parent);
                parent.setPageId(pageParent);
                parent.setParentPage(-1);
                nextLevel.add(parent);
            }
            currentLevel = nextLevel;
            level++;
        }
        root = currentLevel.get(0);
    }

    // Εισαγωγή ενός Entry σε targetLevel (για reinsert ή bulkLoad)
    private void insertEntry(Node R, Entry E, int targetLevel) throws IOException {
        if (R.getLevel() == targetLevel) {
            R.addEntry(E);
            R.recomputeMBRUpward();
            indexFile.writeNode(R.getPageId(), R);
            adjustTree(R);
            if (R.getEntries().size() > M) {
                handleOverflow(R);
            }
            return;
        }
        Entry best = null;
        double bestInc  = Double.POSITIVE_INFINITY;
        double bestArea = Double.POSITIVE_INFINITY;

        for (Entry c : R.getEntries()) {
            if (!c.isInternalEntry()) continue;
            Node child = indexFile.readNode(c.getChildPage());
            if (child.getParentPage() != R.getPageId()) {
                child.setParentPage(R.getPageId());
                indexFile.writeNode(child.getPageId(), child);
            }

            double inc  = c.getMBR().enlargement(E.getMBR());
            double area = c.getMBR().area();
            int childSz = child.getEntries().size();

            Node bestChildNode = (best == null
                    ? null
                    : indexFile.readNode(best.getChildPage()));
            int bestChildSz = (bestChildNode == null
                    ? Integer.MAX_VALUE
                    : bestChildNode.getEntries().size());

            if (inc < bestInc
                    || (inc == bestInc && area < bestArea)
                    || (inc == bestInc && area == bestArea && childSz < bestChildSz)) {
                best = c;
                bestInc = inc;
                bestArea = area;
            }
        }

        if (best == null) {
            throw new IllegalStateException(
                    "insertEntry: Δεν βρέθηκε internal entry σε κόμβο επιπέδου " + R.getLevel());
        }
        insertEntry(indexFile.readNode(best.getChildPage()), E, targetLevel);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Split κόμβου N σε N1, N2 και ενημέρωση parent (ή δημιουργία νέου root).
    // ─────────────────────────────────────────────────────────────────────────
    private void splitNode(Node N) throws IOException {
        SplitResult sr = chooseSplit(N);

        Node N1 = new Node(N.getLevel(), N.isLeaf());
        Node N2 = new Node(N.getLevel(), N.isLeaf());
        for (Entry e : sr.getGroup1()) {
            N1.addEntry(e);
        }
        for (Entry e : sr.getGroup2()) {
            N2.addEntry(e);
        }
        N1.recomputeMBRUpward();
        N2.recomputeMBRUpward();

        // Αν N είναι root -> δημιουργία νέας ρίζας
        if (N.getPageId() == root.getPageId()) {
            int pageN1 = indexFile.writeNode(-1, N1);
            N1.setPageId(pageN1);
            N1.setParentPage(-1);

            int pageN2 = indexFile.writeNode(-1, N2);
            N2.setPageId(pageN2);
            N2.setParentPage(-1);

            Node newRoot = new Node(N.getLevel() + 1, false);
            newRoot.addEntry(new Entry(N1.getMBR(), pageN1));
            newRoot.addEntry(new Entry(N2.getMBR(), pageN2));
            newRoot.recomputeMBRUpward();
            int newRootPage = indexFile.writeNode(-1, newRoot);
            newRoot.setPageId(newRootPage);
            newRoot.setParentPage(-1);
            root = newRoot;
            return;
        }

        // Αν N δεν είναι root
        int parentPage = N.getParentPage();
        if (parentPage < 0) {
            throw new IllegalStateException(
                    "splitNode: Καλείσαι να σπάσεις κόμβο με parentPage = -1, αλλά δεν είσαι root.");
        }
        Node parent = indexFile.readNode(parentPage);

        // Αφαιρούμε entry του N από parent
        Entry toRemove = null;
        for (Entry e : parent.getEntries()) {
            if (e.isInternalEntry() && e.getChildPage() == N.getPageId()) {
                toRemove = e;
                break;
            }
        }
        if (toRemove != null) {
            parent.getEntries().remove(toRemove);
        }

        // Εγκαθιστούμε N1, N2 ως παιδιά του parent
        int pageN1 = indexFile.writeNode(-1, N1);
        N1.setPageId(pageN1);
        N1.setParentPage(parentPage);

        int pageN2 = indexFile.writeNode(-1, N2);
        N2.setPageId(pageN2);
        N2.setParentPage(parentPage);

        parent.addEntry(new Entry(N1.getMBR(), pageN1));
        parent.addEntry(new Entry(N2.getMBR(), pageN2));
        parent.recomputeMBRUpward();
        indexFile.writeNode(parentPage, parent);
        adjustTree(parent);

        // Αν ο parent overflowάρει, κάνε split κι αυτόν
        if (parent.getEntries().size() > M) {
            splitNode(parent);
        }
    }

    /**
     * Επιλογή optimal split (R*-tree: ελάχιστο sum of margins).
     * Επιστρέφει SplitResult με δύο λίστες entries.
     */
    private SplitResult chooseSplit(Node N) {
        int dim = DIM;
        double bestMarginSum = Double.POSITIVE_INFINITY;
        SplitResult bestSplit = null;

        for (int d = 0; d < dim; d++) {
            final int dimIndex = d;

            List<Entry> sortByMin = new ArrayList<>(N.getEntries());
            sortByMin.sort(Comparator.comparingDouble(e -> e.getMBR().getMin()[dimIndex]));

            List<Entry> sortByMax = new ArrayList<>(N.getEntries());
            sortByMax.sort(Comparator.comparingDouble(e -> e.getMBR().getMax()[dimIndex]));

            for (int which = 0; which < 2; which++) {
                List<Entry> sorted = (which == 0) ? sortByMin : sortByMax;
                double marginSumLocal = 0.0;
                double bestMarginLocal = Double.POSITIVE_INFINITY;
                int bestKLocal = -1;
                int entryCount = sorted.size();

                for (int k = m; k <= entryCount - m; k++) {
                    MBR mbr1 = null, mbr2 = null;
                    for (int i = 0; i < k; i++) {
                        if (mbr1 == null) {
                            mbr1 = sorted.get(i).getMBR().clone();
                        } else {
                            mbr1 = MBR.union(mbr1, sorted.get(i).getMBR());
                        }
                    }
                    for (int i = k; i < entryCount; i++) {
                        if (mbr2 == null) {
                            mbr2 = sorted.get(i).getMBR().clone();
                        } else {
                            mbr2 = MBR.union(mbr2, sorted.get(i).getMBR());
                        }
                    }
                    double margin = mbr1.margin() + mbr2.margin();
                    marginSumLocal += margin;
                    if (margin < bestMarginLocal) {
                        bestMarginLocal = margin;
                        bestKLocal = k;
                    }
                }

                if (marginSumLocal < bestMarginSum) {
                    bestMarginSum = marginSumLocal;
                    List<Entry> sortedBest = sorted;
                    int k = bestKLocal;
                    List<Entry> group1 = new ArrayList<>(sortedBest.subList(0, k));
                    List<Entry> group2 = new ArrayList<>(sortedBest.subList(k, sortedBest.size()));
                    bestSplit = new SplitResult(group1, group2);
                }
            }
        }
        return bestSplit;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Queries: rangeQuery, kNNQuery, skylineQuery.
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Range query: επιστρέφει List<RecordPointer> όλων των leaf entries
     * των οποίων οι συντεταγμένες εμπίπτουν εντός [minCoords, maxCoords].
     */
    public List<RecordPointer> rangeQuery(double[] minCoords, double[] maxCoords) throws IOException {
        MBR queryMBR = new MBR(minCoords, maxCoords);
        List<RecordPointer> results = new ArrayList<>();
        rangeSearch(root, queryMBR, results);
        return results;
    }

    private void rangeSearch(Node N, MBR query, List<RecordPointer> out) throws IOException {
        if (N.getMBR() == null || !N.getMBR().overlaps(query)) return;
        if (N.isLeaf()) {
            for (Entry e : N.getEntries()) {
                if (e.getMBR().isContainedIn(query)) {
                    out.add(e.getPointer());
                }
            }
        } else {
            for (Entry c : N.getEntries()) {
                if (!c.isInternalEntry()) continue;
                Node child = indexFile.readNode(c.getChildPage());
                if (child.getParentPage() != N.getPageId()) {
                    child.setParentPage(N.getPageId());
                    indexFile.writeNode(child.getPageId(), child);
                }
                rangeSearch(child, query, out);
            }
        }
    }

    /**
     * k-NN query: βρίσκει τα k πλησιέστερα γειτονικά σημεία
     * με χρήση priority queue (distance-based).
     */
    public List<RecordPointer> kNNQuery(double[] queryPt, int k) throws IOException {
        PriorityQueue<NNEntry> pq = new PriorityQueue<>();
        pq.add(new NNEntry(root, root.getMBR().minDist(queryPt)));

        List<RecordPointer> result = new ArrayList<>();
        while (!pq.isEmpty() && result.size() < k) {
            NNEntry top = pq.poll();
            if (top.isNode()) {
                Node n = top.getNode();
                if (n.isLeaf()) {
                    for (Entry e : n.getEntries()) {
                        double d = e.getMBR().minDist(queryPt);
                        pq.offer(new NNEntry(e, d));
                    }
                } else {
                    for (Entry e : n.getEntries()) {
                        if (!e.isInternalEntry()) continue;
                        Node child = indexFile.readNode(e.getChildPage());
                        if (child.getParentPage() != n.getPageId()) {
                            child.setParentPage(n.getPageId());
                            indexFile.writeNode(child.getPageId(), child);
                        }
                        double d = e.getMBR().minDist(queryPt);
                        pq.offer(new NNEntry(child, d));
                    }
                }
            } else {
                result.add(top.getEntry().getPointer());
            }
        }
        return result;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SkylineQuery: O(n log n) υλοποίηση 2D skyline (μέσω απλού scan + sort).
    // ─────────────────────────────────────────────────────────────────────────
    /**
     * Υπολογίζει το skyline από όλα τα records του DataFile.
     */
    public List<RecordPointer> skylineQuery() throws IOException {
        List<PointRP> points = new ArrayList<>();

        FileChannel channel = dataFile.getChannel();
        int blockSize = DataFile.BLOCK_SIZE; // 32 KB
        long fileSize = channel.size();
        int totalBlocks = (int) (fileSize / blockSize);
        int dim = dataFile.getDimension();
        int slotsPerBlock = dataFile.getSlotsPerBlock();
        int recordSize = dataFile.getRecordSize();

        for (int blkId = 0; blkId < totalBlocks; blkId++) {
            long blockOffset = (long) blkId * blockSize;

            // 1) Διαβάζουμε το header (live count) των πρώτων 4 bytes του block
            ByteBuffer headerBuf = ByteBuffer.allocate(4);
            channel.read(headerBuf, blockOffset);
            headerBuf.flip();
            int live = headerBuf.getInt(); // valid slots σε αυτό το block

            // 2) Σκανάρουμε μόνο τα slots [0 .. live-1]
            for (int slot = 0; slot < live; slot++) {
                long slotPos = blockOffset + 4L + (long) slot * recordSize;

                // (α) Διαβάζουμε το id
                ByteBuffer idBuf = ByteBuffer.allocate(8);
                channel.read(idBuf, slotPos);
                idBuf.flip();
                long id = idBuf.getLong();

                // Αν id <= 0 (θεωρήθηκε διαγραμμένο ή κενό), το αγνοούμε
                if (id <= 0L) continue;

                // (β) Διαβάζουμε τις συντεταγμένες μετά το id (8 bytes) + όνομα (256 bytes)
                long coordsPos = slotPos + 8 + 256;
                ByteBuffer coordsBuf = ByteBuffer.allocate(8 * dim);
                channel.read(coordsBuf, coordsPos);
                coordsBuf.flip();
                double[] coords = new double[dim];
                for (int i = 0; i < dim; i++) {
                    coords[i] = coordsBuf.getDouble();
                }

                // Προσθέτουμε το σημείο στη λίστα
                RecordPointer rp = new RecordPointer(blkId, slot);
                points.add(new PointRP(coords, rp));
            }
        }

        // 3) Ταξινόμηση κατά coords[0] (και tie-break κατά coords[1])
        points.sort((a, b) -> {
            int cmp = Double.compare(a.coords[0], b.coords[0]);
            if (cmp != 0) return cmp;
            return Double.compare(a.coords[1], b.coords[1]);
        });

        // 4) Ένα πέρασμα για nondominated σύμφωνα με coords[1]
        List<RecordPointer> skyline = new ArrayList<>();
        double bestY = Double.POSITIVE_INFINITY;
        for (PointRP pr : points) {
            double y = pr.coords[1];
            if (y < bestY) {
                skyline.add(pr.rp);
                bestY = y;
            }
        }
        return skyline;
    }

    /** Εσωτερική helper κλάση για skylineQuery: συνδυάζει coords + RecordPointer. */
    private static class PointRP {
        double[] coords;
        RecordPointer rp;
        PointRP(double[] c, RecordPointer r) {
            this.coords = c;
            this.rp = r;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Βοηθητικές κλάσεις / μέθοδοι: NNEntry, adjustTree
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Ενημερώνει αναδρομικά τα MBR όλων των κόμβων από τον κόμβο n έως τη ρίζα.
     * Κάθε φορά:
     *   – Διαβάζουμε τον γονέα,
     *   – Αναζητούμε ποια εγγραφή του γονέα δείχνει στο n,
     *   – Αντιστοιχούμε το MBR της εγγραφής στο MBR του n,
     *   – Γράφουμε τον γονέα πίσω στο IndexFile.
     */
    private void adjustTree(Node n) throws IOException {
        int currentPage = n.getPageId();
        int parentPage = n.getParentPage();

        while (parentPage >= 0) {
            Node parent = indexFile.readNode(parentPage);
            boolean updated = false;

            for (Entry e : parent.getEntries()) {
                if (e.isInternalEntry() && e.getChildPage() == currentPage) {
                    e.setMBR(n.getMBR());  // Αντιστοιχούμε MBR
                    updated = true;
                    break;
                }
            }
            if (updated) {
                parent.recomputeMBRUpward();
                indexFile.writeNode(parentPage, parent);
            }

            // Προχωράμε έναν κόμβο πάνω
            currentPage = parent.getPageId();
            parentPage = parent.getParentPage();
            n = parent;
        }
        // Τέλος, ανανεώνουμε τη ρίζα
        root = indexFile.readNode(currentPage);
    }
}
