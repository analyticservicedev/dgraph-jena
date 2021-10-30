package aas.dgraph.jena.v2;

import aas.dgraph.jena.StorageDgraphDB;
import aas.dgraph.jena.store.DgraphQuadTable;
import aas.dgraph.jena.store.DgraphTripleTable;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.riot.other.G;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.sparql.core.DatasetGraphTriplesQuads;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.sparql.core.TransactionalNotSupported;

import java.util.Iterator;

public class DgraphDB extends DatasetGraphTriplesQuads {
    private StorageDgraphDB storageDgraphDB;
    private final Transactional txn = new TransactionalNotSupported();
    private DatasetPrefixesDgraphDB prefixes;

    public DgraphDB(StorageDgraphDB storageDgraphDB, DatasetPrefixesDgraphDB prefixes) {
        this.storageDgraphDB = storageDgraphDB;
        this.prefixes = prefixes;
    }

    public DgraphTripleTable getTripleTable() {
        return this.storageDgraphDB.getTripleTable();
    }

    public DgraphQuadTable getQuadTable() {
        return this.storageDgraphDB.getQuadTable();
    }

    protected Iterator<Quad> findInDftGraph(Node s, Node p, Node o) {
        return G.triples2quadsDftGraph(this.getTripleTable().find(s, p, o));
    }


    protected Iterator<Quad> findInSpecificNamedGraph(Node g, Node s, Node p, Node o) {
        return this.getQuadTable().find(g, s, p, o);
    }

    protected Iterator<Quad> findInAnyNamedGraphs(Node s, Node p, Node o) {
        return this.getQuadTable().find(Node.ANY, s, p, o);
    }


    protected void addToDftGraph(Node s, Node p, Node o) {
        this.getTripleTable().add(s, p, o);
    }

    protected void addToNamedGraph(Node g, Node s, Node p, Node o) {
        this.getQuadTable().add(g, s, p, o);
    }

    protected void deleteFromDftGraph(Node s, Node p, Node o) {
        this.getTripleTable().delete(s, p, o);
    }

    protected void deleteFromNamedGraph(Node g, Node s, Node p, Node o) {
        this.getQuadTable().delete(g, s, p, o);
    }


    @Override
    public Graph getDefaultGraph() {
        return new DgraphGraphView(this, null);
    }

    @Override
    public Graph getGraph(Node graphNode) {
        return new DgraphGraphView(this, graphNode);
    }

    @Override
    public Iterator<Node> listGraphNodes() {
        return this.storageDgraphDB.getTripleTable().findAll();
    }


    public PrefixMap prefixes() {
        return this.getStoragePrefixes().getPrefixMap();
    }

    public DatasetPrefixesDgraphDB getStoragePrefixes() {
        return this.prefixes;
    }

    public void setDefaultGraph(Graph g) {
        throw new UnsupportedOperationException("Can't set default graph via GraphStore on a TDB-backed dataset");
    }

    public void begin() {
        this.txn.begin();
    }

    public void begin(TxnType txnType) {
        this.txn.begin(txnType);
    }

    public void begin(ReadWrite mode) {
        this.txn.begin(mode);
    }

    public boolean promote(Promote txnType) {
        return this.txn.promote(txnType);
    }

    public void commit() {
        this.txn.commit();
    }

    public void abort() {
        this.txn.abort();
    }

    public boolean isInTransaction() {
        return this.txn.isInTransaction();
    }

    public void end() {
        this.txn.end();
    }

    public ReadWrite transactionMode() {
        return this.txn.transactionMode();
    }

    public TxnType transactionType() {
        return this.txn.transactionType();
    }

    public boolean supportsTransactions() {
        return true;
    }

    public boolean supportsTransactionAbort() {
        return false;
    }
}
