package aas.dgraph.jena;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.StoragePrefixes;
import org.apache.jena.dboe.transaction.txn.Transaction;
import org.apache.jena.dboe.transaction.txn.TransactionException;
import org.apache.jena.dboe.transaction.txn.TransactionalSystem;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.riot.system.PrefixEntry;
import org.apache.jena.riot.system.PrefixLib;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StoragePrefixesDgraphDB implements StoragePrefixes {
    private TransactionalSystem txnSystem;

    public StoragePrefixesDgraphDB(TransactionalSystem txnSystem) {
        this.txnSystem = txnSystem;
    }


    public String get(Node graphNode, String prefix) {
        this.requireTxn();
        graphNode = PrefixLib.canonicalGraphName(graphNode);
        Node p = NodeFactory.createLiteral(prefix);
        Iterator<Tuple<Node>> iter = this.find(new Node[]{graphNode, p, null});
        if (!iter.hasNext()) {
            return null;
        } else {
            Node x = (Node) ((Tuple) iter.next()).get(2);
            Iter.close(iter);
            return x.getURI();
        }
    }

    private Iterator<Tuple<Node>> find(Node[] nodes) {
        // TODO tofix
        return new ArrayList<Tuple<Node>>().iterator();
    }

    public Iterator<PrefixEntry> get(Node graphNode) {
        this.requireTxn();
        graphNode = PrefixLib.canonicalGraphName(graphNode);
        Iterator<Tuple<Node>> iter = this.find(new Node[]{graphNode, null, null});
        return Iter.iter(iter).map((t) -> {
            return PrefixEntry.create(((Node) t.get(1)).getLiteralLexicalForm(), ((Node) t.get(2)).getURI());
        });
    }

    public Iterator<Node> listGraphNodes() {
        this.requireTxn();
        Iterator<Tuple<Node>> iter = this.find(new Node[]{(Node) null, null, null});
        return Iter.iter(iter).map((t) -> {
            return (Node) t.get(0);
        }).distinct();
    }

    public void add(Node graphNode, String prefix, String iriStr) {
        this.ensureWriteTxn();
        this.add_ext(graphNode, prefix, iriStr);
    }

    public void add_ext(Node graphNode, String prefix, String iriStr) {
        graphNode = PrefixLib.canonicalGraphName(graphNode);
        Node p = NodeFactory.createLiteral(prefix);
        Node u = NodeFactory.createURI(iriStr);
        this.remove_ext(graphNode, p, Node.ANY);
        this.addRow(new Node[]{graphNode, p, u});
    }

    // TODO to fix
    private void addRow(Node[] nodes) {

    }

    public void delete(Node graphNode, String prefix) {
        Node p = NodeFactory.createLiteral(prefix);
        this.remove(graphNode, p, (Node) null);
    }

    public void deleteAll(Node graphNode) {
        this.remove(graphNode, (Node) null, (Node) null);
    }

    private void remove(Node g, Node p, Node u) {
        this.ensureWriteTxn();
        this.remove_ext(g, p, u);
    }

    private void remove_ext(Node g, Node p, Node u) {
        g = PrefixLib.canonicalGraphName(g);
        Iterator<Tuple<Node>> iter = this.find(new Node[]{g, p, u});
        List<Tuple<Node>> list = Iter.toList(iter);
        Iterator var6 = list.iterator();

        while (var6.hasNext()) {
            Tuple<Node> tuple = (Tuple) var6.next();
            this.deleteRow(new Node[]{(Node) tuple.get(0), (Node) tuple.get(1), (Node) tuple.get(2)});
        }

    }

    // TODO to fix
    private void deleteRow(Node[] nodes) {

    }

    public boolean isEmpty() {
        this.requireTxn();
        return this.isEmpty();
    }

    public int size() {
        this.requireTxn();
        return (int) this.size();
    }

    private void requireTxn() {
    }

    private void ensureWriteTxn() {
        Transaction txn = this.txnSystem.getThreadTransaction();
        if (txn == null) {
            throw new TransactionException("Not in a transaction");
        } else {
            txn.ensureWriteTxn();
        }
    }
}
