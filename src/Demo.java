// Demo.java
//
// Παράδειγμα χρήσης του RStarTree με OSMParser, τυπώνοντας πλήρη Record (id, name, coords)
// αντί για απλούς RecordPointer.
//
//  • Δημιουργεί DataFile, IndexFile και RStarTree.
//  • Καλεί τον OSMParser ώστε να διαβάσει το map.osm και να εισάγει απευθείας
//    τα Record στο DataFile και στο R*-tree (μέσω insertPointer).
//  • Εκτελεί ένα παράδειγμα ερωτήματος περιοχής, k-NN και skyline.
//  • Για κάθε RecordPointer που επιστρέφεται, διαβάζει το Record από το DataFile
//    και τυπώνει το id, το name και τις συντεταγμένες.
//
// Προϋποθέτει τις κλάσεις:
//   • OSMParser (constructor: OSMParser(RStarTree, DataFile) κι έχει μέθοδο parse(String))
//   • RecordPointer, Record, DataFile, IndexFile, RStarTree, Entry, MBR.

import java.io.IOException;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;
import java.util.List;

public class Demo {
    private static final String OSM_FILENAME    = "map.osm";
    private static final String DATAFILE_NAME   = "map.dbf";
    private static final String INDEXFILE_NAME  = "index.idx";
    private static final int    DIMENSIONS      = 2;

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
            double[] minCoords = { 37.98, 23.73 };
            double[] maxCoords = { 37.99, 23.74 };
            System.out.println("\n3) Παράδειγμα Range Query:");
            List<RecordPointer> rangeRes = tree.rangeQuery(minCoords, maxCoords);
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
            double[] queryPt = { 37.975, 23.735 };
            int k = 5;
            System.out.println("\n4) Παράδειγμα k-NN Query:");
            List<RecordPointer> knnRes = tree.kNNQuery(queryPt, k);
            System.out.printf("   %d πλησιέστερα σημεία στο (%.3f, %.3f):%n", k, queryPt[0], queryPt[1]);
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
