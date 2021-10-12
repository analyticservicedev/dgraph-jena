package aas.dgraph.jena.store;

import aas.dgraph.jena.client.DgraphService;
import io.dgraph.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.jena.base.Sys;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DgraphTripleTable {
    private static final Logger logger = LoggerFactory.getLogger(DgraphTripleTable.class);
    private final DgraphService dgraph;

    public DgraphTripleTable(String dgraphEndpoint) {
        this.dgraph = new DgraphService(dgraphEndpoint);
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

    public void delete(Node s, Node p, Node o) {
        logger.info("Delete {} {} {}", s, p, o);
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
        List<Triple> mock = new ArrayList<>();

        Node ns = NodeFactory.createBlankNode("hello");
        Node np = NodeFactory.createURI("http://dgraphjena");
        Node no = NodeFactory.createLiteral("dgraph");

        Triple e = new Triple(ns, np, no);
        mock.add(e);
        return mock.iterator();
    }


    public void clearTriples() {
    }
}
