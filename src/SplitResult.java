// SplitResult.java
//
// Απλή βοηθητική κλάση που κρατάει δύο λίστες Entry (groups) μετά από split.
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
