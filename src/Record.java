// Record.java
//
// Αντικείμενο που αναπαριστά ένα «record» με id, όνομα και d-διαστασιακές συντεταγμένες.
// Χρησιμοποιείται για να γράψουμε/διαβάσουμε την εγγραφή στον DataFile.
import java.util.Arrays;

public class Record {
    private final long id;
    private final String name;
    private final double[] coords;

    public Record(long id, String name, double[] coords) {
        this.id = id;
        this.name = name;
        this.coords = coords.clone();
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public double[] getCoords() {
        return coords.clone();
    }

    @Override
    public String toString() {
        return "Record{id=" + id + ", name='" + name + '\'' +
                ", coords=" + Arrays.toString(coords) + '}';
    }
}
