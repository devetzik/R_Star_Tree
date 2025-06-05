/**
 * Αντιπροσωπεύει μία εγγραφή (entry) σε έναν κόμβο του R*-tree.
 * Μία εγγραφή μπορεί να είναι είτε:
 *   1) Φύλλο (leaf entry): περιέχει ένα RecordPointer προς ένα σημείο στο DataFile.
 *   2) Εσωτερική (internal entry): περιέχει έναν ακέραιο childPage που δείχνει στον υποκόμβο.
 * Κάθε Entry διατηρεί επίσης ένα MBR (Minimum Bounding Rectangle) που
 * οριοθετεί τη γεωγραφική περιοχή που καλύπτει η εγγραφή/υποδέντρο.
 */
public class Entry {
    private MBR mbr;
    private RecordPointer pointer; // leaf-entry (αν non-null)
    private int childPage;         // internal-entry (αν >=0)

    /**
     * Κατασκευαστής για leaf‐entry.
     *
     * @param mbr Το MBR του σημείου (συνήθως ένα σημείο με ίδιες min/max συντεταγμένες).
     * @param rp Ο δείκτης (RecordPointer) στο DataFile που αποθηκεύει το σημείο.
     */
    public Entry(MBR mbr, RecordPointer rp) {
        this.mbr = mbr;
        this.pointer = rp;
        this.childPage = -1;
    }

    /**
     * Κατασκευαστής για internal entry.
     *
     * @param mbr       Το MBR που καλύπτει ολόκληρο το υποδέντρο του παιδιού.
     * @param childPage Ο αριθμός σελίδας (pageId) του παιδικού Node στον IndexFile.
     */
    public Entry(MBR mbr, int childPage) {
        this.mbr = mbr;
        this.childPage = childPage;
        this.pointer = null;
    }

    /**
     * Επιστρέφει true αν αυτή η εγγραφή είναι leaf (σήμα σημείου), διαφορετικά false.
     *
     * @return true αν pointer != null, αλλιώς false.
     */
    public boolean isLeafEntry() {
        return pointer != null;
    }

    /**
     * Επιστρέφει true αν αυτή η εγγραφή είναι internal (υποδέντρο), διαφορετικά false.
     *
     * @return true αν childPage >= 0 false.
     */
    public boolean isInternalEntry() {
        return childPage >= 0;
    }

    /**
     * Επιστρέφει τον RecordPointer αυτού του leaf‐entry.
     *
     * @return Ο RecordPointer του σημείου.
     */
    public RecordPointer getPointer() {
        return pointer;
    }

    /**
     * Επιστρέφει τον αριθμό σελίδας (pageId) του παιδικού κόμβου,
     * αν αυτή η εγγραφή είναι internal.
     *
     * @return Ο ακέραιος κωδικός σελίδας (childPage).
     */
    public int getChildPage() {
        return childPage;
    }

    /**
     * Επιστρέφει το MBR (Minimum Bounding Rectangle) αυτής της εγγραφής.
     *
     * @return Το MBR της εγγραφής.
     */
    public MBR getMBR() {
        return mbr;
    }

    /**
     * Θέτει το MBR της εγγραφής.
     * Χρησιμοποιείται κατά την προώθηση (adjustTree) για ενημέρωση του MBR
     * στη λίστα του γονέα.
     *
     * @param m Το νέο MBR που θέλουμε να αντιστοιχίσουμε.
     */
    public void setMBR(MBR m) {
        this.mbr = m;
    }
}
