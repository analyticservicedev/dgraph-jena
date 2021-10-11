package aas.dgraph.jena;

import aas.dgraph.jena.store.DgraphQuadTable;
import aas.dgraph.jena.store.DgraphTripleTable;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.StorageRDF;
import org.apache.jena.dboe.transaction.txn.Transaction;
import org.apache.jena.dboe.transaction.txn.TransactionalSystem;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb2.store.NodeId;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class StorageDgraphDB implements StorageRDF {
    private DgraphTripleTable tripleTable;
    private DgraphQuadTable quadTable;
    private TransactionalSystem txnSystem;

    public StorageDgraphDB(TransactionalSystem txnSystem, DgraphTripleTable tripleTable, DgraphQuadTable quadTable) {
        this.txnSystem = txnSystem;
        this.tripleTable = tripleTable;
        this.quadTable = quadTable;
    }

    public DgraphQuadTable getQuadTable() {
        this.checkActive();
        return this.quadTable;
    }

    public DgraphTripleTable getTripleTable() {
        this.checkActive();
        return this.tripleTable;
    }

    private void checkActive() {
    }

    private final void notifyAdd(Node g, Node s, Node p, Node o) {
    }

    private final void notifyDelete(Node g, Node s, Node p, Node o) {
    }

    public void add(Node s, Node p, Node o) {
        this.checkActive();
        this.ensureWriteTxn();
        this.notifyAdd((Node) null, s, p, o);
        this.getTripleTable().add(s, p, o);
    }

    public void add(Node g, Node s, Node p, Node o) {
        this.checkActive();
        this.ensureWriteTxn();
        this.notifyAdd(g, s, p, o);
        this.getQuadTable().add(g, s, p, o);
    }

    public void delete(Node s, Node p, Node o) {
        this.checkActive();
        this.ensureWriteTxn();
        this.notifyDelete((Node) null, s, p, o);
        this.getTripleTable().delete(s, p, o);
    }

    public void delete(Node g, Node s, Node p, Node o) {
        this.checkActive();
        this.ensureWriteTxn();
        this.notifyDelete(g, s, p, o);
        this.getQuadTable().delete(g, s, p, o);
    }

    public void removeAll(Node s, Node p, Node o) {
        this.checkActive();
        this.ensureWriteTxn();
        this.removeWorker(() -> {
//            return this.tripleTable.findAsNodeIds(new Node[]{s, p, o});
            return null;
        }, (x) -> {
//            this.tripleTable.delete(x);
        });
    }

    public void removeAll(Node g, Node s, Node p, Node o) {
        this.checkActive();
        this.ensureWriteTxn();
        this.removeWorker(() -> {
//            return this.quadTable.getNodeTupleTable().findAsNodeIds(new Node[]{g, s, p, o});
            return null;
        }, (x) -> {
//            this.quadTable.getNodeTupleTable().getTupleTable().delete(x);
        });
    }

    private void removeWorker(Supplier<Iterator<Tuple<NodeId>>> finder, Consumer<Tuple<NodeId>> deleter) {
        Object[] buffer = new Object[1000];

        int idx;
        do {
            Iterator<Tuple<NodeId>> iter = (Iterator) finder.get();

            for (idx = 0; idx < 1000 && iter.hasNext(); ++idx) {
                buffer[idx] = iter.next();
            }

            for (int i = 0; i < idx; ++i) {
                Tuple<NodeId> x = (Tuple) buffer[i];
                deleter.accept(x);
                buffer[i] = null;
            }
        } while (idx >= 1000);

    }

    public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
        this.checkActive();
        this.requireTxn();
        return this.getQuadTable().find(g, s, p, o);
    }

    public Iterator<Triple> find(Node s, Node p, Node o) {
        this.checkActive();
        this.requireTxn();
        return this.getTripleTable().find(s, p, o);
    }

    public boolean contains(Node s, Node p, Node o) {
        this.checkActive();
        this.requireTxn();
        return this.getTripleTable().find(s, p, o).hasNext();
    }

    public boolean contains(Node g, Node s, Node p, Node o) {
        this.checkActive();
        this.requireTxn();
        return this.getQuadTable().find(g, s, p, o).hasNext();
    }

    private void requireTxn() {
    }

    private void ensureWriteTxn() {
        Transaction txn = this.txnSystem.getThreadTransaction();
        txn.ensureWriteTxn();
    }
}
