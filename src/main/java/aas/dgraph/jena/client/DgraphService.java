package aas.dgraph.jena.client;

import aas.dgraph.jena.store.DgraphTripleTable;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphGrpc;
import io.dgraph.DgraphProto;
import io.dgraph.Transaction;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.jena.graph.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DgraphService {
    private static final Logger logger = LoggerFactory.getLogger(DgraphService.class);
    private DgraphClient client;

    public DgraphService(String endpoint) {
        ManagedChannel channel1 = ManagedChannelBuilder
                .forTarget(endpoint)
                .usePlaintext().build();
        DgraphGrpc.DgraphStub stub1 = DgraphGrpc.newStub(channel1);
        this.client = new DgraphClient(stub1);
        this.printVersion();
    }

    public void printVersion() {
        DgraphProto.Version v = client.checkVersion();
        logger.info("Client Version {}", v);
    }

    public void queryRdf() {
        String q = Gql.load("gql/demordf.gql");
        Transaction trans = client.newReadOnlyTransaction();
        DgraphProto.Response resp = trans.queryRDF(q);
        String rdfs = resp.getRdf().toStringUtf8();
        logger.info("{}", rdfs);
    }

    public void schema() {
        Transaction rt = client.newReadOnlyTransaction();
        DgraphProto.Response resp = rt.queryRDF("schema{}");
        String json = resp.getJson().toStringUtf8();
        logger.info("{}", json);
    }

    public static void main(String[] args) {
        DgraphService ds = new DgraphService("localhost:9080");
        ds.queryRdf();
    }

    public void add(Node s, Node p, Node o) {
        try {
            logger.info("SPO Name {} {} {}", s.toString(), p.toString(), o.toString());
            Transaction trans = client.newTransaction();
            String xidQuery = Gql.render(Gql.load("gql/xid.gql"), "subject", s.toString());
            logger.info("xid Query: {}", xidQuery);
            String setQuery = Gql.render(Gql.load("gql/setWithXid.gql"),
                    "subject", Gql.wellFormatValue(s.toString()),
                    "predict", Gql.wellFormatPredict(p.toString()),
                    "object", Gql.wellFormatValue(o.toString()));
            logger.info("set Query: {}", setQuery);

            trans.doRequest(DgraphProto.Request.newBuilder()
                    .setCommitNow(true)
                    .setQuery(xidQuery)
                    .addMutations(DgraphProto.Mutation.newBuilder()
                            .setSetNquads(ByteString.copyFromUtf8(setQuery))
                            .build())
                    .build());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.toString());
        }
    }
}
