// RStarTree.java
//
// Κύρια κλάση του R*-tree, χωρίς inline υλοποίηση cache.
// Χρησιμοποιεί την NodeCache για όλα τα read/write των κόμβων.

import java.io.IOException;
import java.util.*;

public class RStarTree {
    private final int DIM;
    private final int M = 50;   // max entries per node
    private final int m = 25;   // min entries after split

    private final DataFile dataFile;
    private final IndexFile indexFile;
    private final NodeCache cache;

    private Node root;

    /**
     * Κατασκευαστής R*-tree.
     * Αρχικοποιεί dataFile, indexFile, δημιουργεί ένα κενό root (leaf) και το γράφει,
     * τοποθετώντας το παράλληλα στην cache ως clean.
     *
     * @param d       Διάσταση (π.χ. 2 για γεωχωρικούς κόμβους)
     * @param df      Το DataFile που χειρίζεται τις εγγραφές (Record) στο δίσκο
     * @param idx     Το IndexFile που χειρίζεται τους κόμβους (Nodes) του R*-tree
     * @throws IOException αν αποτύχει κάποιο I/O
     */
    public RStarTree(int d, DataFile df, IndexFile idx) throws IOException {
        this.DIM       = d;
        this.dataFile  = df;
        this.indexFile = idx;
        // Θέτουμε capacity=500 (μπορείτε να το προσαρμόσετε π.χ. σε 300–1000)
        this.cache     = new NodeCache(indexFile, 500);

        // Δημιουργούμε κενό root (leaf)
        Node newRoot = new Node(0, true);
        int rootPage = indexFile.writeNode(-1, newRoot);
        newRoot.setPageId(rootPage);
        newRoot.setParentPage(-1);
        this.root = newRoot;

        // Αποθηκεύουμε τον root στην cache (clean)
        cache.store(newRoot);
        // Σημ.: store() σημαίνει dirty=true, αλλά επειδή μόλις γράψαμε στον δίσκο,
        //       τον επανασηματοδοτούμε clean:
        try {
            cache.flushAll();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //                       Βασικές λειτουργίες Insert / Split
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Εισαγωγή ενός Record:
     * 1) Εισαγωγή στο dataFile → παίρνουμε RecordPointer
     * 2) Δημιουργία νέου leaf-entry (MBR + pointer)
     * 3) chooseLeaf → επιλέγουμε φύλλο
     * 4) leaf.addEntry + store cache
     * 5) αν overflow, handleOverflow
     */
    public RecordPointer insert(Record rec) throws IOException {
        // 1) Εισαγωγή στο DataFile
        RecordPointer rp = dataFile.insertRecord(rec);

        // 2) Δημιουργία μινιμαλιστικού MBR
        MBR singleMBR = new MBR(rec.getCoords(), rec.getCoords());
        Entry newEntry = new Entry(singleMBR, rp);

        // 3) Βρίσκουμε το κατάλληλο φύλλο
        Node leaf = chooseLeaf(root, newEntry);

        // 4) Προσθέτουμε το entry και σημειώνουμε τον leaf ως dirty
        leaf.addEntry(newEntry);
        cache.store(leaf);

        // 5) Αν υπερβαίνει το M → overflow
        if (leaf.getEntries().size() > M) {
            handleOverflow(leaf);
        }

        // 6) Αν το root άλλαξε (ουσιαστικά δημιουργήθηκε νέος root σε split)
        if (root.getParentPage() >= 0) {
            root = cache.fetch(root.getParentPage());
        }
        return rp;
    }

    /**
     * Επιλογή φύλλου (chooseLeaf).
     * Κάθε φορά που κατεβάζουμε παιδί, το παίρνουμε από cache ή δίσκο,
     * βάζουμε σωστό parentPage και το σημειώνουμε dirty.
     */
    private Node chooseLeaf(Node curr, Entry e) throws IOException {
        if (curr.isLeaf()) {
            return curr;
        }
        Entry best = null;
        double bestInc  = Double.POSITIVE_INFINITY;
        double bestArea = Double.POSITIVE_INFINITY;

        // 1) Προσπαθούμε να βρούμε child με σωστό επίπεδο
        for (Entry c : curr.getEntries()) {
            if (!c.isInternalEntry()) continue;
            int childPage = c.getChildPage();
            Node child = cache.fetch(childPage);
            if (child == null) continue;

            // Ενημέρωση parentPage (αν δεν έχει ήδη)
            child.setParentPage(curr.getPageId());
            cache.store(child);

            if (child.getLevel() >= curr.getLevel()) {
                double inc   = c.getMBR().enlargement(e.getMBR());
                double area  = c.getMBR().area();
                int childSz  = child.getEntries().size();
                if (inc < bestInc
                        || (inc == bestInc && area < bestArea)
                        || (inc == bestInc && area == bestArea
                        && childSz < sizeOfChild(
                        best == null ? c.getChildPage() : best.getChildPage()))) {
                    best     = c;
                    bestInc  = inc;
                    bestArea = area;
                }
            }
        }

        // 2) Fallback: οποιοδήποτε internal child
        if (best == null) {
            bestInc  = Double.POSITIVE_INFINITY;
            bestArea = Double.POSITIVE_INFINITY;
            for (Entry c : curr.getEntries()) {
                if (!c.isInternalEntry()) continue;
                int childPage = c.getChildPage();
                Node child = cache.fetch(childPage);
                if (child == null) continue;

                child.setParentPage(curr.getPageId());
                cache.store(child);

                double inc   = c.getMBR().enlargement(e.getMBR());
                double area  = c.getMBR().area();
                int childSz  = child.getEntries().size();
                if (inc < bestInc
                        || (inc == bestInc && area < bestArea)
                        || (inc == bestInc && area == bestArea
                        && childSz < sizeOfChild(
                        best == null ? c.getChildPage() : best.getChildPage()))) {
                    best     = c;
                    bestInc  = inc;
                    bestArea = area;
                }
            }
        }

        if (best == null) {
            throw new IllegalStateException(
                    "chooseLeaf: Δεν βρέθηκε internal entry σε κόμβο επιπέδου "
                            + curr.getLevel());
        }
        return chooseLeaf(cache.fetch(best.getChildPage()), e);
    }

    /**
     * Επιστρέφει πόσες εγγραφές έχει ο child node, χρησιμοποιώντας cache.
     */
    private int sizeOfChild(int childPage) throws IOException {
        if (childPage < 0) return 0;
        Node child = cache.fetch(childPage);
        return (child == null ? 0 : child.getEntries().size());
    }

    /**
     * Αντιμετώπιση overflow: είτε reinsert είτε split.
     */
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

    /**
     * Επιστρέφει πάντα false, ώστε κάθε εσωτερικός κόμβος να κάνει τουλάχιστον μια reinsert.
     */
    private boolean hasBeenReinserted(Node N) {
        return false;
    }

    /**
     * Πολιτική reinsert: αφαιρούμε το 30% των πιο «μακριών» entries και τα επανεισάγουμε.
     */
    private void reinsert(Node N) throws IOException {
        int p = (int) Math.floor(0.3 * M);
        double[] centroid = new double[DIM];
        Arrays.fill(centroid, 0.0);

        // Υπολογίζουμε το centroid
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

        // Ταξινομούμε descend με βάση την απόσταση από το centroid
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

        // Αφαιρούμε τα p entries
        List<Entry> toReinsert = new ArrayList<>(sorted.subList(0, p));
        N.getEntries().removeAll(toReinsert);
        N.recomputeMBRUpward();
        cache.store(N);

        // Επανεισάγουμε
        for (Entry e : toReinsert) {
            if (e.isLeafEntry()) {
                insertEntry(root, e, 0);
            } else {
                Node child = cache.fetch(e.getChildPage());
                insertEntry(root, e, child.getLevel());
            }
        }
    }

    /**
     * Bottom-up bulkLoad: διαβάζουμε όλα τα records, φτιάχνουμε πρώτα leaf nodes,
     * τα γράφουμε, μετά ανεβαίνουμε επίπεδο-επίπεδο μέχρι τη ρίζα.
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

        // 2) Πακετάρουμε σε nodes των M entries
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
            cache.store(leaf);  // store = dirty, αλλά θα κάνουμε flush στο τέλος
        }

        // 3) Φτιάχνουμε εσωτερικά επίπεδα
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
                cache.store(parent);
            }
            currentLevel = nextLevel;
            level++;
        }
        root = currentLevel.get(0);
    }

    /**
     * Εισαγωγή έτοιμης Entry (για reinsert ή bulkLoad), σε δεδομένο level.
     */
    private void insertEntry(Node R, Entry E, int targetLevel) throws IOException {
        if (R.getLevel() == targetLevel) {
            R.addEntry(E);
            cache.store(R);
            if (R.getEntries().size() > M) {
                handleOverflow(R);
            }
            return;
        }
        Entry best = null;
        double bestInc  = Double.POSITIVE_INFINITY;
        double bestArea = Double.POSITIVE_INFINITY;

        // 1) Προσπάθεια επιλογής child με level ≥ targetLevel
        for (Entry c : R.getEntries()) {
            if (!c.isInternalEntry()) continue;
            Node child = cache.fetch(c.getChildPage());
            child.setParentPage(R.getPageId());
            cache.store(child);

            if (child.getLevel() >= targetLevel) {
                double inc  = c.getMBR().enlargement(E.getMBR());
                double area = c.getMBR().area();
                int childSz = child.getEntries().size();
                if (inc < bestInc
                        || (inc == bestInc && area < bestArea)
                        || (inc == bestInc && area == bestArea
                        && childSz < sizeOfChild(
                        best == null ? c.getChildPage() : best.getChildPage()))) {
                    best     = c;
                    bestInc  = inc;
                    bestArea = area;
                }
            }
        }

        // 2) Fallback: οποιοδήποτε child
        if (best == null) {
            bestInc  = Double.POSITIVE_INFINITY;
            bestArea = Double.POSITIVE_INFINITY;
            for (Entry c : R.getEntries()) {
                if (!c.isInternalEntry()) continue;
                Node child = cache.fetch(c.getChildPage());
                child.setParentPage(R.getPageId());
                cache.store(child);

                double inc  = c.getMBR().enlargement(E.getMBR());
                double area = c.getMBR().area();
                int childSz = child.getEntries().size();
                if (inc < bestInc
                        || (inc == bestInc && area < bestArea)
                        || (inc == bestInc && area == bestArea
                        && childSz < sizeOfChild(
                        best == null ? c.getChildPage() : best.getChildPage()))) {
                    best     = c;
                    bestInc  = inc;
                    bestArea = area;
                }
            }
        }

        if (best == null) {
            throw new IllegalStateException(
                    "insertEntry: Δεν βρέθηκε internal entry σε κόμβο επιπέδου " + R.getLevel());
        }
        insertEntry(cache.fetch(best.getChildPage()), E, targetLevel);
    }

    /**
     * Διαχωρισμός κόμβου N σε N1, N2 και ενημέρωση parent (ή δημιουργία νέου root).
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

        // Αν N είναι root, φτιάχνουμε νέο root
        if (N.getPageId() == root.getPageId()) {
            int pageN1 = indexFile.writeNode(-1, N1);
            N1.setPageId(pageN1);
            N1.setParentPage(-1);
            cache.store(N1);

            int pageN2 = indexFile.writeNode(-1, N2);
            N2.setPageId(pageN2);
            N2.setParentPage(-1);
            cache.store(N2);

            Node newRoot = new Node(N.getLevel() + 1, false);
            newRoot.addEntry(new Entry(N1.getMBR(), pageN1));
            newRoot.addEntry(new Entry(N2.getMBR(), pageN2));
            int newRootPage = indexFile.writeNode(-1, newRoot);
            newRoot.setPageId(newRootPage);
            newRoot.setParentPage(-1);
            root = newRoot;
            cache.store(newRoot);

            N1.setParentPage(newRootPage);
            cache.store(N1);

            N2.setParentPage(newRootPage);
            cache.store(N2);
            return;
        }

        // Αν N δεν είναι root, βρίσκουμε parent
        int parentPage = N.getParentPage();
        if (parentPage < 0) {
            throw new IllegalStateException(
                    "splitNode: Καλείσαι να σπάσεις κόμβο με parentPage = -1, αλλά δεν είσαι root.");
        }
        Node parent = cache.fetch(parentPage);

        // 1) Αφαιρούμε από parent το entry που δείχνει στο N
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
        cache.store(parent);

        // 2) Γράφουμε N1, N2 (νέες σελίδες) με parentPage=parentPage
        int pageN1 = indexFile.writeNode(-1, N1);
        N1.setPageId(pageN1);
        N1.setParentPage(parentPage);
        cache.store(N1);

        int pageN2 = indexFile.writeNode(-1, N2);
        N2.setPageId(pageN2);
        N2.setParentPage(parentPage);
        cache.store(N2);

        // 3) Προσθέτουμε νέες εγγραφές στον parent
        parent.addEntry(new Entry(N1.getMBR(), pageN1));
        parent.addEntry(new Entry(N2.getMBR(), pageN2));
        cache.store(parent);

        // 4) Αν ο parent υπερβαίνει M, splitRecursively
        if (parent.getEntries().size() > M) {
            splitNode(parent);
        }
    }

    /**
     * Επιλογή split (δύο ομάδες entries) βάσει αθροίσματος margins.
     */
    private SplitResult chooseSplit(Node N) {
        int dim = DIM;
        double bestMarginSum = Double.POSITIVE_INFINITY;
        SplitResult bestSplit  = null;

        for (int d = 0; d < dim; d++) {
            final int dimIndex = d;

            List<Entry> sortByMin = new ArrayList<>(N.getEntries());
            sortByMin.sort(Comparator.comparingDouble(e -> e.getMBR().getMin()[dimIndex]));

            List<Entry> sortByMax = new ArrayList<>(N.getEntries());
            sortByMax.sort(Comparator.comparingDouble(e -> e.getMBR().getMax()[dimIndex]));

            for (int which = 0; which < 2; which++) {
                List<Entry> sorted = (which == 0) ? sortByMin : sortByMax;
                double marginSumLocal   = 0.0;
                double bestMarginLocal  = Double.POSITIVE_INFINITY;
                int bestKLocal          = -1;
                int entryCount          = sorted.size();

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
                        bestKLocal      = k;
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

    // ─────────────────────────────────────────────────────────────────────────
    //                       Range, kNN και Skyline Queries
    // ─────────────────────────────────────────────────────────────────────────

    /** Αναδρομική RangeQuery. */
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
                Node child = cache.fetch(c.getChildPage());
                child.setParentPage(N.getPageId());
                cache.store(child);
                rangeSearch(child, query, out);
            }
        }
    }

    /** Best-first k-NN query. */
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
                        Node child = cache.fetch(e.getChildPage());
                        child.setParentPage(n.getPageId());
                        cache.store(child);
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

    /** O(n²) Skyline query. */
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
                Node child = cache.fetch(c.getChildPage());
                child.setParentPage(N.getPageId());
                cache.store(child);
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
