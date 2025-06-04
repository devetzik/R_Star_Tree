// OSMParser.java
//
// Χρησιμοποιεί SAX για streaming parsing του αρχείου .osm.
// Κάθε <node> γίνεται Record και εισάγεται στον R*-tree.

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import javax.xml.parsers.*;
import java.io.File;
import java.io.IOException;

public class OSMParser {
    private RStarTree tree;
    private DataFile dataFile;

    public OSMParser(RStarTree tree, DataFile df) {
        this.tree = tree;
        this.dataFile = df;
    }

    public void parse(String filename) throws ParserConfigurationException, SAXException, IOException {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser saxParser = factory.newSAXParser();

        saxParser.parse(new File(filename), new DefaultHandler() {
            private boolean inNode = false;
            private long currentId;
            private double currentLat;
            private double currentLon;
            private String currentName;

            @Override
            public void startElement(String uri, String localName, String qName, Attributes attributes) {
                if (qName.equals("node")) {
                    inNode = true;
                    currentName = "";
                    currentId = Long.parseLong(attributes.getValue("id"));
                    currentLat = Double.parseDouble(attributes.getValue("lat"));
                    currentLon = Double.parseDouble(attributes.getValue("lon"));
                } else if (inNode && qName.equals("tag")) {
                    String k = attributes.getValue("k");
                    if (k != null && k.equals("name")) {
                        currentName = attributes.getValue("v");
                    }
                }
            }

            @Override
            public void endElement(String uri, String localName, String qName) {
                if (qName.equals("node") && inNode) {
                    double[] coords = {currentLat, currentLon};
                    Record rec = new Record(currentId, currentName == null ? "" : currentName, coords);
                    try {
                        tree.insert(rec);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    inNode = false;
                }
            }
        });
    }
}
