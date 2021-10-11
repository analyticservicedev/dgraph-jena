package aas.dgraph.jena.store;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class DgraphQuadTable {
    private static final Logger logger = LoggerFactory.getLogger(DgraphQuadTable.class);
    private final String endpoint;

    public DgraphQuadTable(String dgraphEndpoint) {
        this.endpoint = dgraphEndpoint;
    }

    public void add(Quad quad) {
        this.add(quad.getGraph(), quad.getSubject(), quad.getPredicate(), quad.getObject());
    }

    public void add(Node gn, Triple triple) {
        this.add(gn, triple.getSubject(), triple.getPredicate(), triple.getObject());
    }

    public void add(Node g, Node s, Node p, Node o) {
        logger.info("Add {} {} {} {}", g, s, p, o);
    }

    public void delete(Quad quad) {
        this.delete(quad.getGraph(), quad.getSubject(), quad.getPredicate(), quad.getObject());
    }

    public void delete(Node gn, Triple triple) {
        this.delete(gn, triple.getSubject(), triple.getPredicate(), triple.getObject());
    }

    public void delete(Node g, Node s, Node p, Node o) {
        logger.info("Delete {} {} {} {}", g, s, p, o);
    }

    public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
        logger.info("Find {} {} {} {}", g, s, p, o);
        return Iter.nullIterator();
    }

    public void clearQuads() {
    }
}
