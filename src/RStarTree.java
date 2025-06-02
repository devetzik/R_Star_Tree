// RStarTree.java
//
// Ένα R*-tree για d-διαστασιακά σημεία, υποστηρίζει insert, delete,
// range query, k-NN query, skyline, bulk-load. Έχει διορθωθεί η
// μέθοδος reinsert ώστε να μην προκαλεί σφάλμα “Δεν βρέθηκε κατάλληλο internal entry”
// όταν επανεισάγονται εγγραφές.

import java.io.IOException;
import java.util.*;

public class RStarTree {
    private final int DIM;             // αριθμός διαστάσεων
    private final int M = 50;          // μέγιστος αριθμός entries ανά κόμβο
    private final int m = 25;          // ελάχιστος γεμισμός
    private Node root;
    private final DataFile dataFile;

    public RStarTree(int d, DataFile df) {
        this.DIM = d;
        this.dataFile = df;
        this.root = new Node(0, true);  // αρχικά ο root είναι απλό φύλλο
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  ΕΙΣΑΓΩΓΗ (Insertion)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Εισάγει ένα Record:
     *  α) το γράφει στο DataFile → παίρνει RecordPointer
     *  β) φτιάχνει MBR γύρω από το σημείο (coords, coords)
     *  γ) βάζει νέο Entry στο κατάλληλο φύλλο
     */
    public RecordPointer insert(Record rec) throws IOException {
        // Αποθήκευση στο DataFile
        RecordPointer rp = dataFile.insertRecord(rec);

        // Φτιάχνουμε MBR για το σημείο
        MBR singleMBR = new MBR(rec.getCoords(), rec.getCoords());
        Entry newEntry = new Entry(singleMBR, rp);

        // Βρίσκουμε το φύλλο
        Node leaf = chooseLeaf(root, newEntry);
        leaf.addEntry(newEntry);

        if (leaf.getEntries().size() > M) {
            handleOverflow(leaf);
        }

        // Αν ο root άλλαξε (split), το ανεβάζουμε
        if (root.getParent() != null) {
            root = root.getParent();
        }
        return rp;
    }

    /**
     * Επιλέγει το κατάλληλο φύλλο (level == 0) βάσει ελάχιστης enlargement.
     */
    private Node chooseLeaf(Node curr, Entry e) {
        if (curr.isLeaf()) {
            return curr;
        }
        Entry best = null;
        double bestInc = Double.POSITIVE_INFINITY;
        double bestArea = Double.POSITIVE_INFINITY;

        for (Entry c : curr.getEntries()) {
            double inc = c.getMBR().enlargement(e.getMBR());
            double area = c.getMBR().area();
            if (inc < bestInc
                    || (inc == bestInc && area < bestArea)
                    || (inc == bestInc && area == bestArea
                    && c.getChild().getEntries().size() < best.getChild().getEntries().size())) {
                best = c;
                bestInc = inc;
                bestArea = area;
            }
        }
        return chooseLeaf(best.getChild(), e);
    }

    /**
     * Όταν κόμβος Ν έχει πάνω από M entries:
     *  - Αν είναι root → split
     *  - Αν είναι εσωτερικός (level > 0) και δεν έχει ξανα‐επαναεισαχθεί → reinsert
     *  - Αλλιώς → split
     */
    private void handleOverflow(Node N) {
        if (N == root) {
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
     * Για απλότητα, επιστρέφει πάντα false, ώστε να κάνουμε reinsert κάθε φορά
     * για εσωτερικούς κόμβους. Αν θέλετε αυστηρό policy “μία φορά ανά εισαγωγή”,
     * προσθέστε flag στο Node.
     */
    private boolean hasBeenReinserted(Node N) {
        return false;
    }

    /**
     * Forced reinsert:
     *  1) Υπολογίζει centroid όλων των entries
     *  2) Ταξινομεί κατά φθίνουσα απόσταση MBR‐κέντρου από centroid
     *  3) Αφαιρεί τα p=30% farthest, επανυπολογίζει MBR του N
     *  4) Επανεισάγει τα p entries **από τη ρίζα**, με το σωστό targetLevel
     *     (για leaf‐entries targetLevel=0, για internal‐entries targetLevel=child.level).
     */
    private void reinsert(Node N) {
        int p = (int) Math.floor(0.3 * M);

        // Υπολογισμός centroid
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

        // Ταξινόμηση κατά απόσταση κέντρου (farthest first)
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

        // Διαλέγουμε τα πρώτο p (farthest)
        List<Entry> toReinsert = new ArrayList<>(sorted.subList(0, p));
        N.getEntries().removeAll(toReinsert);
        N.recomputeMBRUpward();

        // Επανεισαγωγή από root με το σωστό targetLevel
        for (Entry e : toReinsert) {
            if (e.isLeafEntry()) {
                // Είναι leaf entry, targetLevel = 0
                insertEntry(root, e, 0);
            } else {
                // Είναι internal entry, targetLevel = child.level
                insertEntry(root, e, e.getChild().getLevel());
            }
        }
    }

    /**
     * Εισαγωγή ενός υπάρχοντος Entry E σε υποδέντρο με root R και επιθυμητό level = targetLevel.
     * - Ψάχνει πρώτα για child με getLevel() ≥ targetLevel, αγνοώντας leaf‐entries (child==null).
     * - Αν δεν βρεθεί κανένα, επιλέγει fallback child βάσει ελάχιστης enlargement, ανάμεσα σε
     *   μη‐null child entries.
     */
    private void insertEntry(Node R, Entry E, int targetLevel) {
        // Αν είμαστε ήδη στο σωστό level, απλώς προσθέτουμε
        if (R.getLevel() == targetLevel) {
            R.addEntry(E);
            if (R.getEntries().size() > M) {
                handleOverflow(R);
            }
            return;
        }

        // 1) Προσπαθούμε να βρούμε child με level >= targetLevel (skip leaf‐entries)
        Entry best = null;
        double bestInc = Double.POSITIVE_INFINITY;
        double bestArea = Double.POSITIVE_INFINITY;
        for (Entry c : R.getEntries()) {
            if (c.getChild() != null && c.getChild().getLevel() >= targetLevel) {
                double inc = c.getMBR().enlargement(E.getMBR());
                double area = c.getMBR().area();
                if (inc < bestInc
                        || (inc == bestInc && area < bestArea)
                        || (inc == bestInc && area == bestArea
                        && c.getChild().getEntries().size() < best.getChild().getEntries().size())) {
                    best = c;
                    bestInc = inc;
                    bestArea = area;
                }
            }
        }

        // 2) Fallback: Αν δεν βρέθηκε κανένας κατάλληλος child, επιλέγουμε ελάχιστο enlargement
        //    ανάμεσα μόνο σε entries με non-null child
        if (best == null) {
            bestInc = Double.POSITIVE_INFINITY;
            bestArea = Double.POSITIVE_INFINITY;
            for (Entry c : R.getEntries()) {
                if (c.getChild() != null) {
                    double inc = c.getMBR().enlargement(E.getMBR());
                    double area = c.getMBR().area();
                    if (inc < bestInc
                            || (inc == bestInc && area < bestArea)
                            || (inc == bestInc && area == bestArea
                            && c.getChild().getEntries().size() < best.getChild().getEntries().size())) {
                        best = c;
                        bestInc = inc;
                        bestArea = area;
                    }
                }
            }
        }

        // Βεβαιωνόμαστε ότι δεν είναι null: πρέπει να υπάρχει τουλάχιστον ένα internal entry.
        if (best == null) {
            throw new IllegalStateException("insertEntry: Δεν βρέθηκε κατάλληλο internal entry "
                    + "σε κόμβο επιπέδου " + R.getLevel()
                    + " (targetLevel=" + targetLevel + ").");
        }

        // 3) Αναδρομή στον επιλεγμένο child
        insertEntry(best.getChild(), E, targetLevel);
    }

    /**
     * Κάνει split στον κόμβο N χρησιμοποιώντας το R*-tree heuristic:
     *   Για κάθε διάσταση d, ταξινομεί κατά min[d] και κατά max[d], δοκιμάζει k=m..(size-m),
     *   υπολογίζει margin1+margin2, επιλέγει το dimension/side με ελάχιστο άθροισμα margins.
     */
    private void splitNode(Node N) {
        SplitResult sr = chooseSplit(N);

        Node N1 = new Node(N.getLevel(), N.isLeaf());
        Node N2 = new Node(N.getLevel(), N.isLeaf());

        for (Entry e : sr.getGroup1()) {
            N1.addEntry(e);
        }
        for (Entry e : sr.getGroup2()) {
            N2.addEntry(e);
        }

        if (N == root) {
            Node newRoot = new Node(N.getLevel() + 1, false);
            newRoot.addEntry(new Entry(N1.getMBR(), N1));
            newRoot.addEntry(new Entry(N2.getMBR(), N2));
            N1.setParent(newRoot);
            N2.setParent(newRoot);
            newRoot.recomputeMBRUpward();
            root = newRoot;
        } else {
            Node parent = N.getParent();
            Entry toRemove = null;
            for (Entry e : parent.getEntries()) {
                if (e.getChild() == N) {
                    toRemove = e;
                    break;
                }
            }
            parent.removeEntry(toRemove);
            parent.addEntry(new Entry(N1.getMBR(), N1));
            parent.addEntry(new Entry(N2.getMBR(), N2));
            N1.setParent(parent);
            N2.setParent(parent);

            if (parent.getEntries().size() > M) {
                handleOverflow(parent);
            }
        }
    }

    /**
     * Εφαρμογή R*-tree split heuristic:
     *   Για κάθε διάσταση d, φτιάχνουμε sortByMin και sortByMax (οριζόντια και κάθετα),
     *   μετά δοκιμάζουμε k=m..(size-m) για να βρούμε το split που ελαχιστοποιεί το άθροισμα των margins.
     *   Χρησιμοποιούμε final int dimIndex = d ώστε να μην έχουμε πρόβλημα με lambda.
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
    //  ΔΙΑΓΡΑΦΗ (Deletion) & CONDENSE-TREE
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Διαγράφει το record που δείχνει το RecordPointer rp με δεδομένο ότι το σημείο είναι coords.
     * Επιστρέφει true αν βρέθηκε+διαγράφηκε.
     */
    public boolean delete(RecordPointer rp, double[] coords) throws IOException {
        MBR searchMBR = new MBR(coords, coords);
        Node leaf = findLeaf(root, rp, searchMBR);
        if (leaf == null) return false;

        Entry toRemove = null;
        for (Entry e : leaf.getEntries()) {
            if (e.isLeafEntry()) {
                RecordPointer p2 = e.getPointer();
                if (p2.getBlockId() == rp.getBlockId() && p2.getSlotId() == rp.getSlotId()) {
                    toRemove = e;
                    break;
                }
            }
        }
        if (toRemove == null) return false;

        leaf.removeEntry(toRemove);
        condenseTree(leaf);

        if (!root.isLeaf() && root.getEntries().size() == 1) {
            Entry sole = root.getEntries().get(0);
            root = sole.getChild();
            root.setParent(null);
        }

        dataFile.deleteRecord(rp);
        return true;
    }

    /**
     * Βρίσκει το φύλλο που περιέχει ακριβώς το RecordPointer rp (με MBR=point coords).
     */
    private Node findLeaf(Node N, RecordPointer rp, MBR searchMBR) {
        if (!N.getMBR().overlaps(searchMBR)) {
            return null;
        }
        if (N.isLeaf()) {
            for (Entry e : N.getEntries()) {
                if (e.isLeafEntry()) {
                    RecordPointer p2 = e.getPointer();
                    if (p2.getBlockId() == rp.getBlockId() && p2.getSlotId() == rp.getSlotId()) {
                        return N;
                    }
                }
            }
            return null;
        } else {
            for (Entry c : N.getEntries()) {
                if (c.getMBR().overlaps(searchMBR)) {
                    Node res = findLeaf(c.getChild(), rp, searchMBR);
                    if (res != null) return res;
                }
            }
            return null;
        }
    }

    /**
     * Μετά τη διαγραφή σε φύλλο, «συμπιέζουμε» το δέντρο:
     *  - Αν κόμβος έχει < m entries, αφαιρείται και μαζεύουμε τα entries του.
     *  - Έπειτα επανεισάγουμε τα orphaned entries.
     */
    private void condenseTree(Node N) throws IOException {
        List<Node> toReinsert = new ArrayList<>();
        Node n = N;
        while (n != root) {
            if (n.underflows(m)) {
                Node parent = n.getParent();
                Entry removeE = null;
                for (Entry e : parent.getEntries()) {
                    if (e.getChild() == n) {
                        removeE = e;
                        break;
                    }
                }
                parent.removeEntry(removeE);
                collectSubtree(n, toReinsert);
                n = parent;
            } else {
                n.recomputeMBRUpward();
                n = n.getParent();
            }
        }
        for (Node orphan : toReinsert) {
            if (orphan.isLeaf()) {
                for (Entry e : orphan.getEntries()) {
                    RecordPointer rp = e.getPointer();
                    Record rec = dataFile.readRecord(rp);
                    insert(rec);
                }
            } else {
                for (Entry e : orphan.getEntries()) {
                    insertEntry(root, e, orphan.getLevel());
                }
            }
        }
    }

    /**
     * Συλλέγει αναδρομικά όλα τα φύλλα ενός subtree και τα προσθέτει στη λίστα out.
     */
    private void collectSubtree(Node N, List<Node> out) {
        if (N.isLeaf()) {
            out.add(N);
        } else {
            for (Entry e : N.getEntries()) {
                collectSubtree(e.getChild(), out);
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  RANGE QUERY
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Επιστρέφει όλα τα RecordPointer που βρίσκονται εντός του υπερορθογωνίου [minCoords, maxCoords].
     */
    public List<RecordPointer> rangeQuery(double[] minCoords, double[] maxCoords) {
        MBR queryRect = new MBR(minCoords, maxCoords);
        List<RecordPointer> results = new ArrayList<>();
        rangeSearch(root, queryRect, results);
        return results;
    }

    private void rangeSearch(Node N, MBR query, List<RecordPointer> out) {
        if (!N.getMBR().overlaps(query)) return;
        if (N.isLeaf()) {
            for (Entry e : N.getEntries()) {
                if (e.getMBR().isContainedIn(query)) {
                    out.add(e.getPointer());
                }
            }
        } else {
            for (Entry c : N.getEntries()) {
                if (c.getMBR().overlaps(query)) {
                    rangeSearch(c.getChild(), query, out);
                }
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  k-NN Query (Best-First)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Επιστρέφει έως k εγγραφές πλησιέστερες στο queryPt (Euclidean).
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
                        double d = e.getMBR().minDist(queryPt);
                        pq.offer(new NNEntry(e.getChild(), d));
                    }
                }
            } else {
                result.add(top.getEntry().getPointer());
            }
        }
        return result;
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Skyline Query (Brute-Force)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Επιστρέφει τις εγγραφές που είναι στο skyline (κανένα άλλο σημείο δεν τις κυριαρχεί).
     */
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

    private void collectAllLeaves(Node N, List<Entry> out) {
        if (N.isLeaf()) {
            out.addAll(N.getEntries());
        } else {
            for (Entry c : N.getEntries()) {
                collectAllLeaves(c.getChild(), out);
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

    // ─────────────────────────────────────────────────────────────────────────
    //  BULK-LOAD (Bottom-Up STR-like)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Bottom‐up bulk‐load (STR‐like): sort‐by‐first‐coordinate, pack in leaves,
     * έπειτα οικοδόμηση ανώτερων επιπέδων.
     */
    public void bulkLoad(List<Record> records) throws IOException {
        records.sort(Comparator.comparingDouble(r -> r.getCoords()[0]));

        List<Entry> leafEntries = new ArrayList<>();
        for (Record rec : records) {
            RecordPointer rp = dataFile.insertRecord(rec);
            MBR mbr = new MBR(rec.getCoords(), rec.getCoords());
            leafEntries.add(new Entry(mbr, rp));
        }

        List<Node> leaves = new ArrayList<>();
        for (int i = 0; i < leafEntries.size(); i += M) {
            int end = Math.min(i + M, leafEntries.size());
            Node leaf = new Node(0, true);
            for (int j = i; j < end; j++) {
                leaf.addEntry(leafEntries.get(j));
            }
            leaves.add(leaf);
        }

        int level = 1;
        List<Node> currentLevel = leaves;
        while (currentLevel.size() > 1) {
            List<Node> nextLevel = new ArrayList<>();
            for (int i = 0; i < currentLevel.size(); i += M) {
                int end = Math.min(i + M, currentLevel.size());
                Node parent = new Node(level, false);
                for (int j = i; j < end; j++) {
                    Node child = currentLevel.get(j);
                    parent.addEntry(new Entry(child.getMBR(), child));
                }
                nextLevel.add(parent);
            }
            currentLevel = nextLevel;
            level++;
        }
        root = currentLevel.get(0);
        root.setParent(null);
    }

    /** Επιστρέφει τον τρέχοντα root. */
    public Node getRoot() {
        return root;
    }
}
