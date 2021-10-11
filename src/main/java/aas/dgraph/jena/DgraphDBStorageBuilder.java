package aas.dgraph.jena;

import aas.dgraph.jena.store.DgraphQuadTable;
import aas.dgraph.jena.store.DgraphTripleTable;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.dboe.storage.StoragePrefixes;
import org.apache.jena.dboe.transaction.txn.*;
import org.apache.jena.dboe.transaction.txn.journal.Journal;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderLib;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.apache.jena.sparql.sse.SSEParseException;
import org.apache.jena.tdb2.solver.OpExecutorTDB2;
import org.apache.jena.tdb2.sys.ComponentIdMgr;
import org.apache.jena.tdb2.sys.SystemTDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.UUID;

public class DgraphDBStorageBuilder {
    private static Logger log = LoggerFactory.getLogger(DgraphDBStorageBuilder.class);
    private FileFilter fileFilterNewDB = (pathname) -> {
        String fn = pathname.getName();
        if (!fn.equals(".") && !fn.equals("..")) {
            if (pathname.isDirectory()) {
                return true;
            } else if (fn.equals("tdb.cfg")) {
                return false;
            } else {
                return !fn.equals("tdb.lock");
            }
        } else {
            return false;
        }
    };
    private final Location location;
    private final TransactionalSystem txnSystem;
    private final ComponentIdMgr componentIdMgr;
    private final Collection<TransactionalComponent> components = new ArrayList();
    private final Collection<TransactionListener> listeners = new ArrayList();
    private static boolean warnAboutOptimizer = true;

    public static DatasetGraphDgraphDB build(Location location) {
        TransactionCoordinator txnCoord = buildTransactionCoordinator(location);
        TransactionalSystem txnSystem = new TransactionalBase(txnCoord);
        DgraphDBStorageBuilder builder = new DgraphDBStorageBuilder(txnSystem, location, new ComponentIdMgr(UUID.randomUUID()));
        StorageDgraphDB storage = builder.buildStorage();
        StoragePrefixes prefixes = builder.buildPrefixes();
        Objects.requireNonNull(txnCoord);
        builder.components.forEach(txnCoord::add);
        builder.listeners.forEach(txnCoord::addListener);
        txnCoord.start();
        ReorderTransformation reorderTranform = chooseReorderTransformation(location);
        DatasetGraphDgraphDB dsg = new DatasetGraphDgraphDB(location, reorderTranform, storage, prefixes, txnSystem);
        // ???
        QC.setFactory(dsg.getContext(), OpExecutorTDB2.OpExecFactoryTDB);

        return dsg;
    }


    private static TransactionCoordinator buildTransactionCoordinator(Location location) {
        Journal journal = Journal.create(location);
        TransactionCoordinator txnCoord = new TransactionCoordinator(journal);
        return txnCoord;
    }



    private static void error(Logger log, String msg) throws DgraphDBException {
        if (log != null) {
            log.error(msg);
        }
        throw new DgraphDBException(msg);
    }

    private DgraphDBStorageBuilder(TransactionalSystem txnSystem, Location location, ComponentIdMgr componentIdMgr) {
        this.txnSystem = txnSystem;
        this.location = location;
        this.componentIdMgr = componentIdMgr;
    }

    private StorageDgraphDB buildStorage() {
        DgraphTripleTable tripleTable = this.buildTripleTable();
        DgraphQuadTable quadTable = this.buildQuadTable();
        StorageDgraphDB dsg = new StorageDgraphDB(this.txnSystem, tripleTable, quadTable);
        return dsg;
    }

    private StoragePrefixes buildPrefixes() {
        StoragePrefixesDgraphDB prefixes = this.buildPrefixTable();
        return prefixes;
    }

    private DgraphTripleTable buildTripleTable() {
        DgraphTripleTable tripleTable = new DgraphTripleTable();
        return tripleTable;
    }

    private DgraphQuadTable buildQuadTable() {
        DgraphQuadTable tripleTable = new DgraphQuadTable();
        return tripleTable;
    }

    private StoragePrefixesDgraphDB buildPrefixTable() {
        StoragePrefixesDgraphDB x = new StoragePrefixesDgraphDB(this.txnSystem);
        return x;
    }


    public static ReorderTransformation chooseReorderTransformation(Location location) {
        if (location == null) {
            return ReorderLib.identity();
        } else {
            ReorderTransformation reorder = null;
            if (location.exists("stats.opt")) {
                try {
                    reorder = ReorderLib.weighted(location.getPath("stats.opt"));
                    log.debug("Statistics-based BGP optimizer");
                } catch (SSEParseException var3) {
                    log.warn("Error in stats file: " + var3.getMessage());
                    reorder = null;
                }
            }

            if (reorder == null && location.exists("fixed.opt")) {
                reorder = ReorderLib.fixed();
                log.debug("Fixed pattern BGP optimizer");
            }

            if (location.exists("none.opt")) {
                reorder = ReorderLib.identity();
                log.debug("Optimizer explicitly turned off");
            }

            if (reorder == null) {
                reorder = SystemTDB.getDefaultReorderTransform();
            }

            if (reorder == null && warnAboutOptimizer) {
                ARQ.getExecLogger().warn("No BGP optimizer");
            }

            return reorder;
        }
    }
}
