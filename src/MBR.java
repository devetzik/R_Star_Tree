public class MBR implements Cloneable {
    private double[] min;  // χαμηλότερες συντεταγμένες
    private double[] max;  // υψηλότερες συντεταγμένες

    /**
     * Δημιουργεί νέο MBR με τις δύο γωνίες (min, max).
     *
     * @param min Δικτυακές συντεταγμένες (χαμηλά όρια).
     * @param max Δικτυακές συντεταγμένες (υψηλά όρια).
     */
    public MBR(double[] min, double[] max) {
        this.min = min.clone();
        this.max = max.clone();
    }

    /** @return Πίνακας με τις χαμηλότερες συντεταγμένες. */
    public double[] getMin() {
        return min;
    }

    /** @return Πίνακας με τις υψηλότερες συντεταγμένες. */
    public double[] getMax() {
        return max;
    }

    /** Υπολογίζει και επιστρέφει το εμβαδόν (area) του MBR (προϊόν διαφορών). */
    public double area() {
        double prod = 1.0;
        for (int i = 0; i < min.length; i++) {
            prod *= (max[i] - min[i]);
        }
        return prod;
    }

    /**
     * Υπολογίζει πόσο μεγαλώνει το MBR αν το ενώσουμε με το other.
     *
     * @param other Το άλλο MBR για σύγκριση.
     * @return Η διαφορά (area(union) - area(original)).
     */
    public double enlargement(MBR other) {
        MBR u = union(this, other);
        return u.area() - this.area();
    }

    /** Ελέγχει αν το MBR αυτό τέμνεται με το άλλο. */
    public boolean overlaps(MBR other) {
        for (int i = 0; i < min.length; i++) {
            if (this.max[i] < other.min[i] || other.max[i] < this.min[i]) {
                return false;
            }
        }
        return true;
    }

    /** Ελέγχει αν το MBR αυτό περιέχεται πλήρως μέσα στο other. */
    public boolean isContainedIn(MBR other) {
        for (int i = 0; i < min.length; i++) {
            if (this.min[i] < other.min[i] || this.max[i] > other.max[i]) {
                return false;
            }
        }
        return true;
    }

    /** Υπολογίζει την ελάχιστη απόσταση από σημείο (minDist για k-NN). */
    public double minDist(double[] point) {
        double sum = 0.0;
        for (int i = 0; i < min.length; i++) {
            double d = 0.0;
            if (point[i] < min[i]) {
                d = min[i] - point[i];
            } else if (point[i] > max[i]) {
                d = point[i] - max[i];
            }
            sum += d * d;
        }
        return Math.sqrt(sum);
    }

    /** Επιστρέφει αθροιστικό margin (περίμετρο) του MBR. */
    public double margin() {
        double sum = 0.0;
        for (int i = 0; i < min.length; i++) {
            sum += 2.0 * (max[i] - min[i]);
        }
        return sum;
    }

    /** Ενώνει δύο MBR σε νέο MBR που τα περιέχει και τα δύο. */
    public static MBR union(MBR a, MBR b) {
        int dim = a.min.length;
        double[] newMin = new double[dim];
        double[] newMax = new double[dim];
        for (int i = 0; i < dim; i++) {
            newMin[i] = Math.min(a.min[i], b.min[i]);
            newMax[i] = Math.max(a.max[i], b.max[i]);
        }
        return new MBR(newMin, newMax);
    }

    /** Δημιουργεί και επιστρέφει βαθύ αντίγραφο (clone) αυτού του MBR. */
    @Override
    public MBR clone() {
        try {
            MBR copy = (MBR) super.clone();
            copy.min = this.min.clone();
            copy.max = this.max.clone();
            return copy;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError("MBR clone failed", e);
        }
    }
}