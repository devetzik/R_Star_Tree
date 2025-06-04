// Record.java
//
// Αναπαριστά ένα σημείο (node) με id, όνομα, και διάνυσμα συντεταγμένων.
import java.io.Serializable;

public class Record implements Serializable {
    private long id;
    private String name;
    private double[] coords;

    public Record(long id, String name, double[] coords) {
        this.id = id;
        this.name = name;
        this.coords = coords;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public double[] getCoords() {
        return coords;
    }
}

