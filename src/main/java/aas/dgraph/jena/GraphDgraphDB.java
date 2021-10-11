package aas.dgraph.jena;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.dboe.DBOpEnvException;
import org.apache.jena.dboe.storage.StoragePrefixes;
import org.apache.jena.dboe.storage.system.GraphViewStorage;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.other.G;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;

import java.util.Iterator;
import java.util.function.Function;

public class GraphDgraphDB extends GraphViewStorage {
    private final DatasetGraphDgraphDB datasetGraphDgraphDB;

    public static GraphDgraphDB db_createDefaultGraph(DatasetGraphDgraphDB dsg, StoragePrefixes prefixes) {
        return new GraphDgraphDB(dsg, Quad.defaultGraphNodeGenerated, prefixes);
    }

    public static GraphDgraphDB db_createNamedGraph(DatasetGraphDgraphDB dsg, Node graphIRI, StoragePrefixes prefixes) {
        return new GraphDgraphDB(dsg, graphIRI, prefixes);
    }

    public static GraphDgraphDB db_createUnionGraph(DatasetGraphDgraphDB dsg, StoragePrefixes prefixes) {
        return new GraphDgraphDB(dsg, Quad.unionGraph, prefixes);
    }

    private GraphDgraphDB(DatasetGraphDgraphDB dataset, Node graphName, StoragePrefixes prefixes) {
        super(dataset, graphName, prefixes);
        this.datasetGraphDgraphDB = dataset;
    }

    public DatasetGraphDgraphDB getDSG() {
        return this.datasetGraphDgraphDB;
    }

    public PrefixMapping getPrefixMapping() {
        return this.createPrefixMapping();
    }

    protected final int graphBaseSize() {
        return 0;
    }

    private static Iterator<Triple> projectQuadsToTriples(Node graphNode, Iterator<Quad> iter) {
        Function<Quad, Triple> f = (q) -> {
            if (graphNode != null && !q.getGraph().equals(graphNode)) {
                throw new DBOpEnvException("projectQuadsToTriples: Quads from unexpected graph (expected=" + graphNode + ", got=" + q.getGraph() + ")");
            } else {
                return q.asTriple();
            }
        };
        return Iter.map(iter, f);
    }

    protected ExtendedIterator<Triple> graphUnionFind(Node s, Node p, Node o) {
        Node g = Quad.unionGraph;
        Iterator<Quad> iterQuads = this.getDSG().find(g, s, p, o);
        Iterator<Triple> iter = G.quads2triples(iterQuads);
        iter = Iter.distinctAdjacent(iter);
        return WrappedIterator.createNoRemove(iter);
    }

    public final void sync() {
    }

    public final void close() {
        this.sync();
    }
}
