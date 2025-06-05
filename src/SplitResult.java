/**
 * Η κλάση SplitResult αντιπροσωπεύει το αποτέλεσμα ενός split κόμβου σε έναν R*-tree.
 * Κάθε split χωρίζει τα Entry ενός κόμβου σε δύο ομάδες, ώστε να δημιουργηθούν δύο
 * νέοι κόμβοι. Η SplitResult κρατάει τις δύο αυτές λίστες group1 και group2 των Entry.
 */
import java.util.List;

public class SplitResult {
    private final List<Entry> group1;
    private final List<Entry> group2;

    public SplitResult(List<Entry> g1, List<Entry> g2) {
        this.group1 = g1;
        this.group2 = g2;
    }

    public List<Entry> getGroup1() {
        return group1;
    }

    public List<Entry> getGroup2() {
        return group2;
    }
}
