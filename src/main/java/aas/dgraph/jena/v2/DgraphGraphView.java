package aas.dgraph.jena.v2;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.GraphView;

public class DgraphGraphView extends GraphView implements Graph {
    public DgraphGraphView(DgraphDB dgraphDB, Node graphNode) {
        super(dgraphDB, graphNode);
    }
}
