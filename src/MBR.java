// MBR.java
//
// Minimal Bounding Rectangle (Hyper-Rectangle) για d-διαστασιακά αντικείμενα.
// Περιέχει συντεταγμένες min[] και max[] και παρέχει βοηθητικές μεθόδους
// για area(), margin(), enlargement(), overlap(), minDist() κ.λπ.

public class MBR {
    private final double[] min;  // μήκος = d
    private final double[] max;  // μήκος = d

    public MBR(double[] min, double[] max) {
        this.min = min.clone();
        this.max = max.clone();
    }

    public double[] getMin() {
        return min.clone();
    }

    public double[] getMax() {
        return max.clone();
    }

    /** Επιστρέφει το n-διάστατο «όγκο» (area για d=2, volume για d=3 κ.λπ.). */
    public double area() {
        double a = 1.0;
        for (int i = 0; i < min.length; i++) {
            a *= (max[i] - min[i]);
        }
        return a;
    }

    /** Επιστρέφει το άθροισμα των ακμών (margin), που χρησιμοποιείται από R*-tree. */
    public double margin() {
        double sum = 0.0;
        for (int i = 0; i < min.length; i++) {
            sum += (max[i] - min[i]);
        }
        return sum;
    }

    /** Δημιουργεί ένα νέο MBR = ένωση (union) των δύο δεδομένων. */
    public static MBR union(MBR a, MBR b) {
        int d = a.min.length;
        double[] mn = new double[d], mx = new double[d];
        for (int i = 0; i < d; i++) {
            mn[i] = Math.min(a.min[i], b.min[i]);
            mx[i] = Math.max(a.max[i], b.max[i]);
        }
        return new MBR(mn, mx);
    }

    /** Καλογραμμή: επιστρέφει το νέο area - παλιό area. */
    public double enlargement(MBR other) {
        MBR u = union(this, other);
        return u.area() - this.area();
    }

    /**
     * Υπολογίζει τον υπερ-όγκο της τομής (overlap) μεταξύ this και other.
     * Αν δεν υπάρχει τομή, επιστρέφει 0.
     */
    public double overlap(MBR other) {
        double overlapVol = 1.0;
        for (int i = 0; i < min.length; i++) {
            double l = Math.max(this.min[i], other.min[i]);
            double r = Math.min(this.max[i], other.max[i]);
            if (r <= l) return 0.0;  // δεν υπάρχει επικάλυψη
            overlapVol *= (r - l);
        }
        return overlapVol;
    }

    /**
     * Υπολογίζει την ελάχιστη Ευκλείδεια απόσταση από ένα σημείο p (αν είναι εντός του κουτιού,
     * επιστρέφει 0).
     */
    public double minDist(double[] p) {
        double sum = 0.0;
        for (int i = 0; i < p.length; i++) {
            double ri;
            if (p[i] < min[i])        ri = min[i];
            else if (p[i] > max[i])   ri = max[i];
            else                       ri = p[i];
            sum += (p[i] - ri) * (p[i] - ri);
        }
        return Math.sqrt(sum);
    }

    /** Επιστρέφει αν το this MBR περιέχεται πλήρως μέσα στο other. */
    public boolean isContainedIn(MBR other) {
        for (int i = 0; i < min.length; i++) {
            if (this.min[i] < other.min[i] || this.max[i] > other.max[i]) {
                return false;
            }
        }
        return true;
    }

    /** Επιστρέφει αν το this MBR επικαλύπτεται με το other (κοινό σημείο). */
    public boolean overlaps(MBR other) {
        for (int i = 0; i < min.length; i++) {
            if (this.max[i] < other.min[i] || this.min[i] > other.max[i]) {
                return false;
            }
        }
        return true;
    }

    /** Κλωνοποιητής ώστε οι μεταβολές σε αντίγραφο να μην επηρεάζουν το πρωτότυπο. */
    @Override
    public MBR clone() {
        return new MBR(min, max);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("MBR[");
        sb.append("min=").append(java.util.Arrays.toString(min));
        sb.append(", max=").append(java.util.Arrays.toString(max)).append("]");
        return sb.toString();
    }
}
