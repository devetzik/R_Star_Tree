// OSMParser.java
//
// Χρησιμοποιεί SAX parsers της Java για να διαβάσει σε streaming
// ένα αρχείο OSM (XML) και να δημιουργήσει, για κάθε <node>:
//    • id       → χρησιμοποιείται ως long id του Record
//    • lat, lon → δημιουργούμε ένα double[] {lat, lon} ως coords
//    • name     → αν μέσα στο <node> υπάρχει <tag k="name" v="…"/>,
//                  τότε το κρατάμε ως όνομα· αλλιώς όνομα=""
// Έπειτα, φτιάχνουμε Record rec = new Record(id, name, new double[]{lat, lon})
// και καλούμε tree.insert(rec), οπότε αυτόματα γράφεται στο DataFile
// και εισάγεται στο R*-tree.

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.SAXException;
import java.io.File;

public class OSMParser {
    private final RStarTree tree;
    private final DataFile dataFile;

    /**
     * @param tree     Το R*-tree στο οποίο θα εισάγουμε τους κόμβους.
     * @param dataFile Το DataFile που υποστηρίζει το R*-tree (για disk-based storage).
     */
    public OSMParser(RStarTree tree, DataFile dataFile) {
        this.tree = tree;
        this.dataFile = dataFile;
    }

    /**
     * Διαβάζει το αρχείο OSM στο given filename (π.χ. "map.osm") και,
     * για κάθε κόμβο (<node>), δημιουργεί Record και τον εισάγει στο δέντρο.
     *
     * @param filename Το πλήρες μονοπάτι στο osm αρχείο (πρέπει να είναι UTF-8 κωδικοποιημένο).
     */
    public void parse(String filename) throws Exception {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser saxParser = factory.newSAXParser();
        File osmFile = new File(filename);
        saxParser.parse(osmFile, new OSMHandler());
    }

    /**
     * Εσωτερική κλάση που υλοποιεί DefaultHandler του SAX.
     * Κρατάει κατάσταση για κάθε node ώστε να συγκεντρώνει τα attributes
     * id, lat, lon και τα <tag k="name" v="..."/> μέσα στο node.
     */
    private class OSMHandler extends DefaultHandler {
        private boolean inNode = false;
        private long currentId;
        private double currentLat;
        private double currentLon;
        private String currentName;

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes)
                throws SAXException {
            if (qName.equals("node")) {
                // Ξεκινάμε νέο node: διαβάζουμε τα attributes id, lat, lon
                inNode = true;
                currentName = "";  // αν δεν βρούμε <tag k="name">, θα παραμείνει άδειο

                // Το OSM format αποθηκεύει τα id ως String
                String idStr = attributes.getValue("id");
                String latStr = attributes.getValue("lat");
                String lonStr = attributes.getValue("lon");
                if (idStr == null || latStr == null || lonStr == null) {
                    throw new SAXException("Κάποιο <node> έλειπε id/lat/lon attributes.");
                }
                try {
                    currentId = Long.parseLong(idStr);
                    currentLat = Double.parseDouble(latStr);
                    currentLon = Double.parseDouble(lonStr);
                } catch (NumberFormatException ex) {
                    throw new SAXException("Άκυρη μορφή id/lat/lon στο OSM.", ex);
                }

            } else if (inNode && qName.equals("tag")) {
                // Αν είμαστε μέσα σε node και συναντήσουμε <tag> ελέγχουμε αν k="name"
                String k = attributes.getValue("k");
                String v = attributes.getValue("v");
                if (k != null && k.equals("name") && v != null) {
                    currentName = v;
                }
            }
            // Για τα υπόλοιπα στοιχεία (way, relation κ.λπ.) δεν κάνουμε τίποτα
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            if (qName.equals("node") && inNode) {
                // Τέλος του element <node> → φτιάχνουμε Record & το εισάγουμε στο DataFile & στο R*-tree
                inNode = false;
                double[] coords = new double[]{currentLat, currentLon};
                Record rec = new Record(currentId, currentName, coords);
                try {
                    tree.insert(rec);
                } catch (Exception e) {
                    throw new SAXException("Σφάλμα κατά την insert στο R*-tree.", e);
                }
            }
            // Σημείωση: δεν χρειαζόμαστε κάποιο cleanup για tags μέσα σε node,
            // γιατί κρατούμε απλώς ένα currentName κι αυτό αντικαθίσταται όταν βρούμε νέο <node>.
        }
    }
}
