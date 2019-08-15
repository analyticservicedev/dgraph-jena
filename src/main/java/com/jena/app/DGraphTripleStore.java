package com.jena.app;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.TripleStore;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * use DGraph as database.
 * save triples to DGraph.
 */
public class DGraphTripleStore implements TripleStore {
    private DGraph graph;

    private DGraphService dGraphService;

    private final static Logger logger = LoggerFactory.getLogger(DGraphTripleStore.class);

    public DGraphTripleStore(DGraph dGraph) {
        this.graph = dGraph;
        this.dGraphService = new DGraphService(dGraph);
    }

    @Override
    public void close() {
        logger.info("Closing DGraph Triple Store..");
    }

    @Override
    public void add(Triple t) {
        logger.info("Adding Triple {} to Store..", t);
    }

    @Override
    public void delete(Triple t) {
        logger.info("Delete Triple {} from Store..", t);
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean contains(Triple t) {
        return false;
    }

    @Override
    public ExtendedIterator<Node> listSubjects() {
        return null;
    }

    @Override
    public ExtendedIterator<Node> listPredicates() {
        return null;
    }

    @Override
    public ExtendedIterator<Node> listObjects() {
        return null;
    }

    @Override
    public ExtendedIterator<Triple> find(Triple t) {
        return null;
    }

    @Override
    public void clear() {

    }
}
