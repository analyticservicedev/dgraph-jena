package aas.dgraph.jena.store;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class DgraphTripleTable {
    private static final Logger logger = LoggerFactory.getLogger(DgraphTripleTable.class);

    public DgraphTripleTable() {
    }

    public void add(Triple triple) {
        this.add(triple.getSubject(), triple.getPredicate(), triple.getObject());
    }

    public void add(Node s, Node p, Node o) {
        logger.info("Add {} {} {}", s, p, o);
    }

    public void delete(Triple triple) {
        this.delete(triple.getSubject(), triple.getPredicate(), triple.getObject());
    }

    public void delete(Node s, Node p, Node o) {

    }

    public Iterator<Triple> find(Node s, Node p, Node o) {
        logger.info("Find {} {} {}", s, p, o);
        return Iter.nullIterator();
    }

    public void clearTriples() {
    }
}
