package aas.dgraph.jena.store;

import aas.dgraph.jena.client.DgraphService;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.TripleStore;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class DgraphTripleTable implements TripleStore {
    private static final Logger logger = LoggerFactory.getLogger(DgraphTripleTable.class);
    private final DgraphService dgraph;

    public DgraphTripleTable(String dgraphEndpoint) {
        this.dgraph = new DgraphService(dgraphEndpoint);
    }

    @Override
    public void close() {
        logger.info("Close Triple Store");
    }

    public void add(Triple triple) {
        this.add(triple.getSubject(), triple.getPredicate(), triple.getObject());
    }

    public void add(Node s, Node p, Node o) {
        logger.info("Add {} {} {}", s, p, o);
        dgraph.add(s, p, o);
    }

    public void delete(Triple triple) {
        this.delete(triple.getSubject(), triple.getPredicate(), triple.getObject());
    }

    @Override
    public int size() {
        logger.info("Get Size");
        return dgraph.findAllSize();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Triple t) {
        return dgraph.hasTriple(t);
    }

    @Override
    public ExtendedIterator<Node> listSubjects() {
        return dgraph.findAllSubjects();
    }

    // TODO fix
    public ExtendedIterator<Node> findAll() {
        return dgraph.findAllSubjects();
    }

    @Override
    public ExtendedIterator<Node> listPredicates() {
        return dgraph.findAllPredicates();
    }

    @Override
    public ExtendedIterator<Node> listObjects() {
        return dgraph.findAllObjects();
    }

    @Override
    public ExtendedIterator<Triple> find(Triple t) {
        return dgraph.findTriple(t);
    }

    @Override
    public void clear() {
        clearTriples();
    }

    public void delete(Node s, Node p, Node o) {
        logger.info("Delete {} {} {}", s, p, o);
        dgraph.delete(s, p, o);
    }

    /**
     * TODO: query in dgraph
     *
     * @param s
     * @param p
     * @param o
     * @return
     */
    public Iterator<Triple> find(Node s, Node p, Node o) {
        logger.info("Find {} {} {}", s, p, o);
        return dgraph.find(s, p, o);
    }


    public void clearTriples() {
    }
}
