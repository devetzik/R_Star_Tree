// Demo.java
//
// Παράδειγμα χρήσης του OSMParser μαζί με DataFile & RStarTree.
// Διαβάζουμε ένα αρχείο "map.osm" (OpenStreetMap XML), εισάγουμε
// όλους τους κόμβους ως σημειακές εγγραφές στο R*-tree (διάστασης 2).
// Αφαιρέθηκαν όλες οι εμφανίσεις "(null RecordPointer)"—τώρα φιλτράρουμε
// κι εκτυπώνουμε μόνο έγκυρα rp.

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Demo {
    public static void main(String[] args) {
        try {
            // Διάσταση 2: lat / lon
            int d = 2;
            // 1) Φτιάχνουμε (ή ανοίγουμε) το DataFile που θα κρατήσει τις εγγραφές
            DataFile df = new DataFile("map.dbf", d);

            // 2) Δημιουργούμε ένα κενό R*-tree (διάσταση 2)
            RStarTree tree = new RStarTree(d, df);

            // 3) Φτιάχνουμε έναν OSMParser για να γεμίσουμε το δέντρο
            OSMParser parser = new OSMParser(tree, df);

            // 4) Κάνουμε parse το OSM αρχείο. Κάθε <node> γίνεται Record στο δέντρο.
            System.out.println("Ξεκινάω ανάγνωση του map.osm ...");
            parser.parse("map.osm");
            System.out.println("Ολοκληρώθηκε η εισαγωγή των κόμβων στο R*-tree.");

            System.out.println();

            // --- Παραδείγματα queries μετά το parse ---

            // 5) Range query: επιλεγμένο ορθογώνιο (lat,lon), π.χ. [ (37.9,23.6), (38.0,23.8) ]
            double[] minCoords = {41.5005, 26.5005};
            double[] maxCoords = {41.5006, 26.5506};
            List<RecordPointer> inRangeAll = tree.rangeQuery(minCoords, maxCoords);
            // Φιλτράρουμε μόνο τους μη-null rp
            List<RecordPointer> inRange = inRangeAll.stream()
                    .filter(rp -> rp != null)
                    .collect(Collectors.toList());

            System.out.println("Βρέθηκαν " + inRange.size() + " (έγκυροι) κόμβοι μέσα στο ορθογώνιο:");
            if (inRange.isEmpty()) {
                System.out.println("  (κανένας)");
            } else {
                for (RecordPointer rp : inRange) {
                    Record r = df.readRecord(rp);
                    System.out.printf("  id=%d, name=\"%s\", coords=(%.6f,%.6f)%n",
                            r.getId(), r.getName(),
                            r.getCoords()[0], r.getCoords()[1]);
                }
            }

            System.out.println();

            // 6) k-NN query: τα 5 πλησιέστερα σημεία στο (37.975, 23.735)
            double[] queryPt = {41.500555, 26.500555};
            int k = 5;
            List<RecordPointer> nnAll = tree.kNNQuery(queryPt, k);
            // Φιλτράρουμε μόνο τους μη-null rp
            List<RecordPointer> nn = nnAll.stream()
                    .filter(rp -> rp != null)
                    .collect(Collectors.toList());

            System.out.println("Τα " + k + " κοντινότερα σημεία στο (37.975,23.735):");
            if (nn.isEmpty()) {
                System.out.println("  (κανένα διαθέσιμο σημείο)");
            } else {
                // Εάν επιστράφηκαν περισσότερα από k (απλώς guard), κρατάμε πρώτα k
                int toShow = Math.min(k, nn.size());
                for (int i = 0; i < toShow; i++) {
                    RecordPointer rp = nn.get(i);
                    Record r = df.readRecord(rp);
                    System.out.printf("  #%d: id=%d, name=\"%s\", coords=(%.6f,%.6f)%n",
                            i + 1,
                            r.getId(), r.getName(),
                            r.getCoords()[0], r.getCoords()[1]);
                }
                if (nn.size() < k) {
                    System.out.println("  (επιστράφηκαν μόνο " + nn.size() + " από τα " + k + ")");
                }
            }

            System.out.println();

            // 7) Skyline query (σε ολόκληρο το dataset)
            List<RecordPointer> skyAll = tree.skylineQuery();
            // Φιλτράρουμε μόνο τους μη-null rp
            List<RecordPointer> sky = skyAll.stream()
                    .filter(rp -> rp != null)
                    .collect(Collectors.toList());

            System.out.println("Αποτέλεσμα skyline (σημεία που δεν κυριαρχούνται):");
            if (sky.isEmpty()) {
                System.out.println("  (κανένα)");
            } else {
                for (RecordPointer rp : sky) {
                    Record r = df.readRecord(rp);
                    System.out.printf("  id=%d, name=\"%s\", coords=(%.6f,%.6f)%n",
                            r.getId(), r.getName(),
                            r.getCoords()[0], r.getCoords()[1]);
                }
            }

            System.out.println();

            // 8) Κλείνουμε το DataFile
            df.close();

        } catch (IOException ioe) {
            System.err.println("I/O σφάλμα: " + ioe.getMessage());
            ioe.printStackTrace();
        } catch (Exception ex) {
            System.err.println("Γενικό σφάλμα: " + ex.getMessage());
            ex.printStackTrace();
        }
    }
}
