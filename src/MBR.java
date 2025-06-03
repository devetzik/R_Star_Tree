// MBR.java
//
// Minimum Bounding Rectangle: κρατάει πίνακες min[] και max[] διαστάσεων DIM.

import java.util.Arrays;

public class MBR implements Cloneable {
    private double[] min;
    private double[] max;

    public MBR(double[] min, double[] max) {
        this.min = Arrays.copyOf(min, min.length);
        this.max = Arrays.copyOf(max, max.length);
    }

    public double[] getMin() {
        return min;
    }

    public double[] getMax() {
        return max;
    }

    public double area() {
        double a = 1.0;
        for (int i = 0; i < min.length; i++) {
            a *= (max[i] - min[i]);
        }
        return a;
    }

    public double enlargement(MBR other) {
        double[] newMin = new double[min.length];
        double[] newMax = new double[max.length];
        for (int i = 0; i < min.length; i++) {
            newMin[i] = Math.min(min[i], other.min[i]);
            newMax[i] = Math.max(max[i], other.max[i]);
        }
        double newArea = 1.0;
        for (int i = 0; i < newMin.length; i++) {
            newArea *= (newMax[i] - newMin[i]);
        }
        return newArea - area();
    }

    public boolean overlaps(MBR other) {
        for (int i = 0; i < min.length; i++) {
            if (max[i] < other.min[i] || other.max[i] < min[i]) {
                return false;
            }
        }
        return true;
    }

    public boolean isContainedIn(MBR other) {
        for (int i = 0; i < min.length; i++) {
            if (min[i] < other.min[i] || max[i] > other.max[i]) {
                return false;
            }
        }
        return true;
    }

    public double minDist(double[] point) {
        double sum = 0;
        for (int i = 0; i < point.length; i++) {
            double di = 0.0;
            if (point[i] < min[i]) {
                di = min[i] - point[i];
            } else if (point[i] > max[i]) {
                di = point[i] - max[i];
            } else {
                di = 0;
            }
            sum += di * di;
        }
        return Math.sqrt(sum);
    }

    public double margin() {
        double sum = 0;
        for (int i = 0; i < min.length; i++) {
            sum += (max[i] - min[i]);
        }
        return 2.0 * sum;
    }

    @Override
    public MBR clone() {
        return new MBR(min, max);
    }

    public static MBR union(MBR a, MBR b) {
        double[] newMin = new double[a.min.length];
        double[] newMax = new double[a.min.length];
        for (int i = 0; i < a.min.length; i++) {
            newMin[i] = Math.min(a.min[i], b.min[i]);
            newMax[i] = Math.max(a.max[i], b.max[i]);
        }
        return new MBR(newMin, newMax);
    }
}
