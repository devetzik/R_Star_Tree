// RStarTree.java
//
// Κύρια κλάση του R*-tree, χωρίς χρήση cache αλλά με σωστή ενημέρωση parentPage
// κάθε φορά που “κατεβάζουμε” έναν κόμβο από το IndexFile.

import java.io.IOException;
import java.util.*;

public class RStarTree {
    private final int DIM;
    private final int M = 50;
    private final int m = 25;

    private DataFile dataFile;
    private IndexFile indexFile;
    private Node root;

    public RStarTree(int d, DataFile df, IndexFile idx) throws IOException {
        this.DIM = d;
        this.dataFile = df;
        this.indexFile = idx;
        // Δημιουργούμε κενό root (leaf) και το γράφουμε στο indexfile
        Node newRoot = new Node(0, true);
        int rootPage = indexFile.writeNode(-1, newRoot);
        newRoot.setPageId(rootPage);
        newRoot.setParentPage(-1);
        this.root = newRoot;
    }

    /**
     * Εισαγωγή ενός Record: πρώτα στο DataFile → παίρνουμε RecordPointer,
     * μετά φτιάχνουμε νέο leaf‐entry και το εισάγουμε στο R*-tree.
     */
    public RecordPointer insert(Record rec) throws IOException {
        // 1) DataFile
        RecordPointer rp = dataFile.insertRecord(rec);
        // 2) Νέο leaf‐Entry
        MBR singleMBR = new MBR(rec.getCoords(), rec.getCoords());
        Entry newEntry = new Entry(singleMBR, rp);

        // 3) Βρίσκουμε το κατάλληλο φύλλο
        Node leaf = chooseLeaf(root, newEntry);
        leaf.addEntry(newEntry);
        indexFile.writeNode(leaf.getPageId(), leaf);

        if (leaf.getEntries().size() > M) {
            handleOverflow(leaf);
        }

        // 4) Αν το root άλλαξε (parentPage ≥ 0), το φορτώνουμε
        if (root.getParentPage() >= 0) {
            root = indexFile.readNode(root.getParentPage());
        }
        return rp;
    }

    /**
     * Επιλογή φύλλου (chooseLeaf). Κάθε φορά που “κατεβάζουμε” εσωτερικό κόμβο,
     * ενημερώνουμε και το parentPage του παιδιού.
     */
    private Node chooseLeaf(Node curr, Entry e) throws IOException {
        if (curr.isLeaf()) {
            return curr;
        }
        Entry best = null;
        double bestInc = Double.POSITIVE_INFINITY;
        double bestArea = Double.POSITIVE_INFINITY;

        // 1) Προσπάθεια να βρούμε internal child με κατάλληλο επίπεδο
        for (Entry c : curr.getEntries()) {
            if (!c.isInternalEntry()) continue; // φιλτράρουμε leaf‐entries
            int childPage = c.getChildPage();
            Node child = indexFile.readNode(childPage);
            // Ενημέρωση parentPage
            child.setParentPage(curr.getPageId());
            indexFile.writeNode(child.getPageId(), child);

            if (child.getLevel() >= curr.getLevel()) {
                double inc = c.getMBR().enlargement(e.getMBR());
                double area = c.getMBR().area();
                int childSize = child.getEntries().size();
                if (inc < bestInc
                        || (inc == bestInc && area < bestArea)
                        || (inc == bestInc && area == bestArea
                        && childSize < sizeOfChild(best == null ? c.getChildPage() : best.getChildPage()))) {
                    best = c;
                    bestInc = inc;
                    bestArea = area;
                }
            }
        }

        // 2) Fallback: όποιον internal child βρούμε
        if (best == null) {
            bestInc = Double.POSITIVE_INFINITY;
            bestArea = Double.POSITIVE_INFINITY;
            for (Entry c : curr.getEntries()) {
                if (!c.isInternalEntry()) continue;
                int childPage = c.getChildPage();
                Node child = indexFile.readNode(childPage);
                // Ενημέρωση parentPage
                child.setParentPage(curr.getPageId());
                indexFile.writeNode(child.getPageId(), child);

                double inc = c.getMBR().enlargement(e.getMBR());
                double area = c.getMBR().area();
                int childSize = child.getEntries().size();
                if (inc < bestInc
                        || (inc == bestInc && area < bestArea)
                        || (inc == bestInc && area == bestArea
                        && childSize < sizeOfChild(best == null ? c.getChildPage() : best.getChildPage()))) {
                    best = c;
                    bestInc = inc;
                    bestArea = area;
                }
            }
        }

        if (best == null) {
            throw new IllegalStateException(
                    "chooseLeaf: Δεν βρέθηκε internal entry σε κόμβο επιπέδου " + curr.getLevel());
        }
        Node chosenChild = indexFile.readNode(best.getChildPage());
        return chooseLeaf(chosenChild, e);
    }

    private int sizeOfChild(int childPage) throws IOException {
        if (childPage < 0) return 0;
        Node child = indexFile.readNode(childPage);
        return (child == null ? 0 : child.getEntries().size());
    }

    private void handleOverflow(Node N) throws IOException {
        if (N.getPageId() == root.getPageId()) {
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
        // Επιστρέφει false ώστε κάθε εσωτερικός κόμβος να κάνει έστω μία φορά reinsert
        return false;
    }

    /**
     * Πολιτική reinsert:
     * Αφαιρούμε το 30% των πιο “απομακρυσμένων” entries και τα επανεισάγουμε.
     */
    private void reinsert(Node N) throws IOException {
        int p = (int) Math.floor(0.3 * M);
        double[] centroid = new double[DIM];
        Arrays.fill(centroid, 0.0);
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

        List<Entry> sorted = new ArrayList<>(N.getEntries());
        sorted.sort((a, b) -> {
            double da = 0, db = 0;
            double[] ca = new double[DIM], cb = new double[DIM];
            for (int i = 0; i < DIM; i++) {
                ca[i] = (a.getMBR().getMin()[i] + a.getMBR().getMax()[i]) / 2.0;
                cb[i] = (b.getMBR().getMin()[i] + b.getMBR().getMax()[i]) / 2.0;
                da += (ca[i] - centroid[i]) * (ca[i] - centroid[i]);
                db += (cb[i] - centroid[i]) * (cb[i] - centroid[i]);
            }
            return Double.compare(db, da);
        });

        List<Entry> toReinsert = new ArrayList<>(sorted.subList(0, p));
        N.getEntries().removeAll(toReinsert);
        N.recomputeMBRUpward();
        indexFile.writeNode(N.getPageId(), N);

        for (Entry e : toReinsert) {
            if (e.isLeafEntry()) {
                insertEntry(root, e, 0);
            } else {
                Node child = indexFile.readNode(e.getChildPage());
                insertEntry(root, e, child.getLevel());
            }
        }
    }

    /**
     * bulkLoad: bottom‐up packing χωρίς πολλαπλά overflows.
     * Φτιάχνει όλες τις leaf‐nodes, τις γράφει, έπειτα φτιάχνει επίπεδα πάνω.
     */
    public void bulkLoad(List<Record> records) throws IOException {
        records.sort(Comparator.comparingDouble(r -> r.getCoords()[0]));

        // 1) Δημιουργία όλων των leaf entries
        List<Entry> leafEntries = new ArrayList<>();
        for (Record rec : records) {
            RecordPointer rp = dataFile.insertRecord(rec);
            MBR mbr = new MBR(rec.getCoords(), rec.getCoords());
            leafEntries.add(new Entry(mbr, rp));
        }

        // 2) Πακετάρουμε σε leaf nodes (M εγγραφές ανά κόμβο)
        List<Node> leaves = new ArrayList<>();
        for (int i = 0; i < leafEntries.size(); i += M) {
            int end = Math.min(i + M, leafEntries.size());
            Node leaf = new Node(0, true);
            for (int j = i; j < end; j++) {
                leaf.addEntry(leafEntries.get(j));
            }
            int pageLeaf = indexFile.writeNode(-1, leaf);
            leaf.setPageId(pageLeaf);
            leaf.setParentPage(-1);
            leaves.add(leaf);
        }

        // 3) Φτιάχνουμε τα επόμενα επίπεδα μέχρι να φτιάξουμε root
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

    /**
     * Εισαγωγή ήδη έτοιμης Entry (για reinsert ή bulkLoad), σε δεδομένο επίπεδο.
     * Εδώ επίσης ενημερώνουμε parentPage μόλις κατεβάζουμε παιδί.
     */
    private void insertEntry(Node R, Entry E, int targetLevel) throws IOException {
        if (R.getLevel() == targetLevel) {
            R.addEntry(E);
            indexFile.writeNode(R.getPageId(), R);
            if (R.getEntries().size() > M) {
                handleOverflow(R);
            }
            return;
        }
        Entry best = null;
        double bestInc = Double.POSITIVE_INFINITY;
        double bestArea = Double.POSITIVE_INFINITY;

        // 1) Προσπάθεια επιλογής internal children στο σωστό επίπεδο
        for (Entry c : R.getEntries()) {
            if (!c.isInternalEntry()) continue;
            Node child = indexFile.readNode(c.getChildPage());
            // Ενημέρωση parentPage
            child.setParentPage(R.getPageId());
            indexFile.writeNode(child.getPageId(), child);

            if (child.getLevel() >= targetLevel) {
                double inc = c.getMBR().enlargement(E.getMBR());
                double area = c.getMBR().area();
                int childSize = child.getEntries().size();
                if (inc < bestInc
                        || (inc == bestInc && area < bestArea)
                        || (inc == bestInc && area == bestArea
                        && childSize < sizeOfChild(best == null ? c.getChildPage() : best.getChildPage()))) {
                    best = c;
                    bestInc = inc;
                    bestArea = area;
                }
            }
        }

        // 2) Fallback: όποιον internal child βρούμε
        if (best == null) {
            bestInc = Double.POSITIVE_INFINITY;
            bestArea = Double.POSITIVE_INFINITY;
            for (Entry c : R.getEntries()) {
                if (!c.isInternalEntry()) continue;
                Node child = indexFile.readNode(c.getChildPage());
                // Ενημέρωση parentPage
                child.setParentPage(R.getPageId());
                indexFile.writeNode(child.getPageId(), child);

                double inc = c.getMBR().enlargement(E.getMBR());
                double area = c.getMBR().area();
                int childSize = child.getEntries().size();
                if (inc < bestInc
                        || (inc == bestInc && area < bestArea)
                        || (inc == bestInc && area == bestArea
                        && childSize < sizeOfChild(best == null ? c.getChildPage() : best.getChildPage()))) {
                    best = c;
                    bestInc = inc;
                    bestArea = area;
                }
            }
        }

        if (best == null) {
            throw new IllegalStateException(
                    "insertEntry: Δεν βρέθηκε internal entry σε κόμβο επιπέδου " + R.getLevel());
        }
        Node bestChild = indexFile.readNode(best.getChildPage());
        insertEntry(bestChild, E, targetLevel);
    }

    /**
     * Διασπά κόμβο N σε N1, N2 και προσαρμόζει τον γονέα ή, αν N == root, φτιάχνει νέο root.
     * Σημείωση: εδώ δεν υπάρχει cache, οπότε κάθε read/write πάει απευθείας στο indexFile.
     */
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

        // Περίπτωση: N είναι root
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
            int newRootPage = indexFile.writeNode(-1, newRoot);
            newRoot.setPageId(newRootPage);
            newRoot.setParentPage(-1);
            root = newRoot;

            // Ενημέρωση parentPage για N1, N2
            N1.setParentPage(newRootPage);
            indexFile.writeNode(N1.getPageId(), N1);

            N2.setParentPage(newRootPage);
            indexFile.writeNode(N2.getPageId(), N2);
            return;
        }

        // Περίπτωση: N δεν είναι root
        int parentPage = N.getParentPage();
        if (parentPage < 0) {
            throw new IllegalStateException(
                    "splitNode: Καλείσαι να σπάσεις κόμβο με parentPage = -1, αλλά δεν είσαι root.");
        }
        Node parent = indexFile.readNode(parentPage);

        // 1) Αφαιρούμε το entry του N από τον parent
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
        indexFile.writeNode(parent.getPageId(), parent);

        // 2) Γράφουμε N1, N2 με parentPage = parentPage
        int pageN1 = indexFile.writeNode(-1, N1);
        N1.setPageId(pageN1);
        N1.setParentPage(parentPage);

        int pageN2 = indexFile.writeNode(-1, N2);
        N2.setPageId(pageN2);
        N2.setParentPage(parentPage);

        // 3) Προσθέτουμε νέα entries στον parent
        parent.addEntry(new Entry(N1.getMBR(), pageN1));
        parent.addEntry(new Entry(N2.getMBR(), pageN2));
        indexFile.writeNode(parent.getPageId(), parent);

        // 4) Αν ο parent ξεχειλίζει, κάνουμε splitRecursively
        if (parent.getEntries().size() > M) {
            splitNode(parent);
        }
    }

    /**
     * Επιλογή split (δύο ομάδες entries) βάσει αθροίσματος των margins.
     */
    private SplitResult chooseSplit(Node N) {
        int dim = DIM;
        double bestMarginSum = Double.POSITIVE_INFINITY;
        SplitResult bestSplit = null;

        for (int d = 0; d < dim; d++) {
            final int dimIndex = d;

            // Ταξινόμηση κατά min[dimIndex]
            List<Entry> sortByMin = new ArrayList<>(N.getEntries());
            sortByMin.sort(Comparator.comparingDouble(e -> e.getMBR().getMin()[dimIndex]));

            // Ταξινόμηση κατά max[dimIndex]
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
                    List<Entry> sortedBest = (which == 0) ? sortByMin : sortByMax;
                    int k = bestKLocal;
                    List<Entry> group1 = new ArrayList<>(sortedBest.subList(0, k));
                    List<Entry> group2 = new ArrayList<>(sortedBest.subList(k, sortedBest.size()));
                    bestSplit = new SplitResult(group1, group2);
                }
            }
        }
        return bestSplit;
    }

    /** RangeQuery (αναδρομική αναζήτηση). */
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
                if (c.getMBR().overlaps(query)) {
                    Node child = indexFile.readNode(c.getChildPage());
                    // Ενημέρωση parentPage (ασφαλής ακόμη κι αν το child έχει ήδη σωστό parent)
                    child.setParentPage(N.getPageId());
                    indexFile.writeNode(child.getPageId(), child);
                    rangeSearch(child, query, out);
                }
            }
        }
    }

    /** Best‐first k-NN query. */
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
                        double d = e.getMBR().minDist(queryPt);
                        Node child = indexFile.readNode(e.getChildPage());
                        child.setParentPage(n.getPageId());
                        indexFile.writeNode(child.getPageId(), child);
                        pq.offer(new NNEntry(child, d));
                    }
                }
            } else {
                result.add(top.getEntry().getPointer());
            }
        }
        return result;
    }

    /** O(n^2) Skyline. */
    public List<RecordPointer> skylineQuery() throws IOException {
        List<Entry> allEntries = new ArrayList<>();
        collectAllLeaves(root, allEntries);

        List<RecordPointer> skyline = new ArrayList<>();
        for (int i = 0; i < allEntries.size(); i++) {
            double[] p = allEntries.get(i).getMBR().getMin();
            boolean dominated = false;
            for (int j = 0; j < allEntries.size(); j++) {
                if (i == j) continue;
                double[] q = allEntries.get(j).getMBR().getMin();
                if (dominates(q, p)) {
                    dominated = true;
                    break;
                }
            }
            if (!dominated) {
                skyline.add(allEntries.get(i).getPointer());
            }
        }
        return skyline;
    }

    private void collectAllLeaves(Node N, List<Entry> out) throws IOException {
        if (N.isLeaf()) {
            out.addAll(N.getEntries());
        } else {
            for (Entry c : N.getEntries()) {
                if (!c.isInternalEntry()) continue;
                Node child = indexFile.readNode(c.getChildPage());
                child.setParentPage(N.getPageId());
                indexFile.writeNode(child.getPageId(), child);
                collectAllLeaves(child, out);
            }
        }
    }

    private boolean dominates(double[] q, double[] p) {
        boolean strictlyBetter = false;
        for (int i = 0; i < q.length; i++) {
            if (q[i] > p[i]) return false;
            if (q[i] < p[i]) strictlyBetter = true;
        }
        return strictlyBetter;
    }
}
