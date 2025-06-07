import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class OSMParser {
    private final RStarTree tree;
    private final DataFile dataFile;

    /**
     * Ο constructor αποθηκεύει το RStarTree (και το DataFile
     * αν θέλουμε να κάνουμε κάτι απευθείας με το αρχείο).
     */
    public OSMParser(RStarTree tree, DataFile df) {
        this.tree = tree;
        this.dataFile = df;
    }

    /**
     * Διαβάζει το αρχείο OSM και εισάγει κάθε node στο R*-tree.
     * Για κόμβους χωρίς name, δημιουργεί τυχαίο μικρό όνομα.
     */
    public void parse(String filename)
            throws ParserConfigurationException, SAXException, IOException {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser saxParser = factory.newSAXParser();

        saxParser.parse(filename, new DefaultHandler() {
            private boolean inNode = false;
            private long currentId;
            private double currentLat;
            private double currentLon;
            private String currentName;

            @Override
            public void startElement(String uri, String localName,
                                     String qName, Attributes attributes) {
                if ("node".equals(qName)) {
                    inNode = true;
                    // Ξεκινάμε με κενό όνομα. Αν δεν βρεθεί tag name,
                    // θα του αναθέσουμε τυχαίο αργότερα.
                    currentName = "";
                    currentId = Long.parseLong(attributes.getValue("id"));
                    currentLat = Double.parseDouble(attributes.getValue("lat"));
                    currentLon = Double.parseDouble(attributes.getValue("lon"));
                } else if (inNode && "tag".equals(qName)) {
                    String k = attributes.getValue("k");
                    if ("name".equals(k)) {
                        currentName = attributes.getValue("v");
                    }
                }
            }

            @Override
            public void endElement(String uri, String localName, String qName) {
                if ("node".equals(qName) && inNode) {
                    // Αν δεν βρέθηκε άνωθεν όνομα, δημιουργούμε τυχαίο μικρό όνομα
                    if (currentName == null || currentName.isEmpty()) {
                        currentName = generateRandomName();
                    }
                    double[] coords = { currentLat, currentLon };
                    Record rec = new Record(currentId, currentName, coords);
                    try {
                        // Εισάγουμε πρώτα στο DataFile, παίρνουμε RecordPointer
                        RecordPointer rp = dataFile.insertRecord(rec);
                        // Μετά στο R*-tree
                        tree.insertPointer(rp, coords);
                    } catch (IOException e) {
                        // Αν υπήρξε σφάλμα, ρίχνουμε SAXException για να σταματήσει η parse()
                        throw new RuntimeException("Σφάλμα κατά την εισαγωγή στον DataFile/R*-tree", e);
                    }
                    inNode = false;
                }
            }

            /**
             * Δημιουργεί ένα μικρό τυχαίο όνομα μήκους 6 χαρακτήρων
             * από πεζά γράμματα (a–z).
             */
            private String generateRandomName() {
                int length = 6;
                char[] nameChars = new char[length];
                for (int i = 0; i < length; i++) {
                    // πεζά γράμματα a–z
                    nameChars[i] = (char) ('a' + ThreadLocalRandom.current().nextInt(26));
                }
                return new String(nameChars);
            }
        });
    }
}