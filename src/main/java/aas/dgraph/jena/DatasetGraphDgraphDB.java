package aas.dgraph.jena;

import aas.dgraph.jena.store.DgraphQuadTable;
import aas.dgraph.jena.store.DgraphTripleTable;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.dboe.storage.StoragePrefixes;
import org.apache.jena.dboe.storage.system.DatasetGraphStorage;
import org.apache.jena.dboe.transaction.txn.TransactionalSystem;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;

import java.util.Iterator;

public class DatasetGraphDgraphDB extends DatasetGraphStorage {
    private final StorageDgraphDB storageDgraphDB;
    private final Location location;
    private final TransactionalSystem txnSystem;
    private final ReorderTransformation reorderTransformation;
    private boolean isClosed = false;

    public DatasetGraphDgraphDB(
            Location location, ReorderTransformation reorderTransformation,
            StorageDgraphDB storage, StoragePrefixes prefixes, TransactionalSystem txnSystem) {
        super(storage, prefixes, txnSystem);
        this.storageDgraphDB = storage;
        this.location = location;
        this.txnSystem = txnSystem;
        this.reorderTransformation = reorderTransformation;
    }

    private void checkNotClosed() {
        if (this.isClosed) {
            try {
                throw new DgraphDBException("dataset closed");
            } catch (DgraphDBException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean supportsTransactionAbort() {
        return true;
    }

    public Location getLocation() {
        return this.location;
    }

    public DgraphQuadTable getQuadTable() {
        this.checkNotClosed();
        return this.storageDgraphDB.getQuadTable();
    }

    public DgraphTripleTable getTripleTable() {
        this.checkNotClosed();
        return this.storageDgraphDB.getTripleTable();
    }

    public TransactionalSystem getTxnSystem() {
        return this.txnSystem;
    }


    public ReorderTransformation getReorderTransform() {
        return this.reorderTransformation;
    }

    public void close() {
        this.isClosed = true;
        super.close();
    }

    public void shutdown() {
        this.close();
        this.txnSystem.getTxnMgr().shutdown();
    }

    public Graph getDefaultGraph() {
        return this.getDefaultGraphDgraphDB();
    }

    public Graph getGraph(Node graphNode) {
        return this.getGraphTDB(graphNode);
    }

    public Graph getUnionGraph() {
        return this.getUnionGraphTDB();
    }

    public GraphDgraphDB getDefaultGraphDgraphDB() {
        this.checkNotClosed();
        return GraphDgraphDB.db_createDefaultGraph(this, this.getStoragePrefixes());
    }

    public GraphDgraphDB getGraphTDB(Node graphNode) {
        this.checkNotClosed();
        return GraphDgraphDB.db_createNamedGraph(this, graphNode, this.getStoragePrefixes());
    }

    public GraphDgraphDB getUnionGraphTDB() {
        this.checkNotClosed();
        return GraphDgraphDB.db_createUnionGraph(this, this.getStoragePrefixes());
    }

    public Iterator<Node> listGraphNodes() {
        this.checkNotClosed();
        //
        return Iter.nullIter();
    }
}
