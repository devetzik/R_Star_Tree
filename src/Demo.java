//  • Δημιουργεί DataFile, IndexFile και RStarTree.
//  • Καλεί τον OSMParser ώστε να διαβάσει το map.osm και να εισάγει απευθείας
//    τα Record στο DataFile και στο R*-tree (μέσω insertPointer).
//  • Εκτελεί ένα παράδειγμα ερωτήματος περιοχής, k-NN και skyline.
//  • Για κάθε RecordPointer που επιστρέφεται, διαβάζει το Record από το DataFile
//    και τυπώνει το id, το name και τις συντεταγμένες.

// minlat="40.5979960" minlon="22.9641400" maxlat="40.6029480" maxlon="22.9759960"

import java.io.IOException;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;
import java.util.List;

public class Demo {
    private static final String OSM_FILENAME    = "map.osm";
    private static final String DATAFILE_NAME   = "map.dbf";
    private static final String INDEXFILE_NAME  = "index.idx";
    private static final int    DIMENSIONS      = 2;
    private static final double[] MIN_COORDS = { 40.5979960, 22.9641400 };  // Range Query
    private static final double[] MAX_COORDS = { 40.6, 22.97 };  // Range Query
    private static final double[] QUERY_PT = { 40.5979, 22.9645 };  // k-NN


    public static void main(String[] args) {
        DataFile df = null;
        IndexFile idx = null;
        try {
            System.out.println("1) Δημιουργία DataFile, IndexFile και RStarTree...");
            df = new DataFile(DATAFILE_NAME, DIMENSIONS);
            idx = new IndexFile(INDEXFILE_NAME, DIMENSIONS);
            RStarTree tree = new RStarTree(DIMENSIONS, df, idx);

            // 2) Χρήση OSMParser για απευθείας εισαγωγή κόμβων (nodes) στο DataFile και στο R*-tree.
            System.out.println("\n2) Ανάγνωση map.osm και εισαγωγή στο R*-tree...");
            OSMParser parser = new OSMParser(tree, df);
            parser.parse(OSM_FILENAME);
            System.out.println("   Ολοκληρώθηκε ingestion από OSMParser.");

            // 3) Παράδειγμα Range Query:
            System.out.println("\n3) Παράδειγμα Range Query:");
            List<RecordPointer> rangeRes = tree.rangeQuery(MIN_COORDS, MAX_COORDS);
            System.out.printf("   Βρέθηκαν %d σημεία εντός ορθογωνίου.%n", rangeRes.size());
            for (int i = 0; i < Math.min(rangeRes.size(), 5); i++) {
                RecordPointer rp = rangeRes.get(i);
                Record rec = df.readRecord(rp);
                System.out.printf("     #%d: id=%d, name=\"%s\", coords=(%.6f, %.6f)%n",
                        i + 1, rec.getId(), rec.getName(),
                        rec.getCoords()[0], rec.getCoords()[1]);
            }
            if (rangeRes.size() > 5) {
                System.out.println("     (εμφανίζονται μόνο τα πρώτα 5)");
            }

            // 4) Παράδειγμα k-NN Query:
            int k = 5;
            System.out.println("\n4) Παράδειγμα k-NN Query:");
            List<RecordPointer> knnRes = tree.kNNQuery(QUERY_PT, k);
            System.out.printf("   %d πλησιέστερα σημεία στο (%.6f, %.6f):%n", k, QUERY_PT[0], QUERY_PT[1]);
            for (int i = 0; i < knnRes.size(); i++) {
                RecordPointer rp = knnRes.get(i);
                Record rec = df.readRecord(rp);
                System.out.printf("     #%d: id=%d, name=\"%s\", coords=(%.6f, %.6f)%n",
                        i + 1, rec.getId(), rec.getName(),
                        rec.getCoords()[0], rec.getCoords()[1]);
            }

            // 5) Παράδειγμα Skyline Query (2D):
            System.out.println("\n5) Παράδειγμα Skyline Query:");
            List<RecordPointer> skyRes = tree.skylineQuery();
            System.out.printf("   Βρέθηκαν %d σημεία skyline.%n", skyRes.size());
            for (int i = 0; i < Math.min(skyRes.size(), 10); i++) {
                RecordPointer rp = skyRes.get(i);
                Record rec = df.readRecord(rp);
                System.out.printf("     #%d: id=%d, name=\"%s\", coords=(%.6f, %.6f)%n",
                        i + 1, rec.getId(), rec.getName(),
                        rec.getCoords()[0], rec.getCoords()[1]);
            }
            if (skyRes.size() > 10) {
                System.out.println("     (εμφανίζονται μόνο τα πρώτα 10)");
            }

            // 6) Κλείσιμο όλων των αρχείων
            df.close();
            idx.close();
            System.out.println("\n== Demo ολοκληρώθηκε ==");
        }
        catch (IOException | ParserConfigurationException | SAXException e) {
            e.printStackTrace();
        } finally {
            try {
                if (df != null) df.close();
                if (idx != null) idx.close();
            } catch (IOException ignored) {}
        }
    }
}
