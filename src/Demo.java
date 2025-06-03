// Demo.java
//
// Παράδειγμα χρήσης: Δημιουργεί DataFile, IndexFile, R*-tree, εκτελεί insert από map.osm,
// τρέχει range, k-NN και skyline queries και τυπώνει αποτελέσματα.

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.List;

public class Demo {
    public static void main(String[] args) {
        try {
            // 1) Δημιουργία DataFile (δεδομένα) και IndexFile (index)
            String dataFilename = "map.dbf";
            String indexFilename = "index.idx";
            int dimension = 2; // latitude, longitude

            DataFile df = new DataFile(dataFilename, dimension);
            IndexFile idx = new IndexFile(indexFilename, dimension);
            RStarTree tree = new RStarTree(dimension, df, idx);

            // 2) Parse OSM και insert όλων των κόμβων
            System.out.println("Ξεκινάω ανάγνωση του map.osm ...");
            OSMParser parser = new OSMParser(tree, df);
            parser.parse("map.osm");
            System.out.println("Ολοκληρώθηκε η εισαγωγή των κόμβων στο R*-tree.");

            // 3) Range Query παράδειγμα
            double[] minCoords = {37.9, 23.6};
            double[] maxCoords = {38.0, 23.8};
            List<RecordPointer> inRange = tree.rangeQuery(minCoords, maxCoords);
            System.out.println("Range Query: Βρέθηκαν " + inRange.size() + " σημεία:");
            if (inRange.isEmpty()) {
                System.out.println("  (κανένα)");
            } else {
                for (RecordPointer rp : inRange) {
                    Record r = df.readRecord(rp);
                    System.out.printf("  id=%d, name=\"%s\", coords=(%.6f, %.6f)%n",
                            r.getId(), r.getName(), r.getCoords()[0], r.getCoords()[1]);
                }
            }

            // 4) k-NN Query παράδειγμα (k=5)
            double[] queryPt = {37.975, 23.735};
            int k = 5;
            List<RecordPointer> nn = tree.kNNQuery(queryPt, k);
            System.out.println("k-NN Query (k=" + k + "):");
            if (nn.isEmpty()) {
                System.out.println("  (κανένα)");
            } else {
                int rank = 1;
                for (RecordPointer rp : nn) {
                    Record r = df.readRecord(rp);
                    System.out.printf("  #%d: id=%d, name=\"%s\", coords=(%.6f, %.6f)%n",
                            rank++, r.getId(), r.getName(),
                            r.getCoords()[0], r.getCoords()[1]);
                }
            }

            // 5) Skyline Query παράδειγμα
            List<RecordPointer> sky = tree.skylineQuery();
            System.out.println("Skyline Query: Σημεία που δεν κυριαρχούνται:");
            if (sky.isEmpty()) {
                System.out.println("  (κανένα)");
            } else {
                for (RecordPointer rp : sky) {
                    Record r = df.readRecord(rp);
                    System.out.printf("  id=%d, name=\"%s\", coords=(%.6f, %.6f)%n",
                            r.getId(), r.getName(), r.getCoords()[0], r.getCoords()[1]);
                }
            }

            // 6) Κλείσιμο αρχείων
            df.close();
            idx.close();

        } catch (ParserConfigurationException | IOException e) {
            e.printStackTrace();
        } catch (org.xml.sax.SAXException e) {
            e.printStackTrace();
        }
    }
}
