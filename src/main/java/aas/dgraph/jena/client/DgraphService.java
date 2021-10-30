package aas.dgraph.jena.client;

import com.google.protobuf.ByteString;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphGrpc;
import io.dgraph.DgraphProto;
import io.dgraph.Transaction;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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

    public List<String> predicates() {
        Transaction rt = client.newReadOnlyTransaction();
        DgraphProto.Response resp = rt.query("schema{}");
        String jsonStr = resp.getJson().toStringUtf8();
        JsonObject json = JSON.parse(jsonStr);
        List<String> preds = json.getArray("schema")
                .map(x -> x.getAsObject().get("predicate").getAsString().value())
                .filter(s -> !s.startsWith("dgraph."))
                .map(Gql::wellFormatPredict)
                .collect(Collectors.toList());
        rt.close();
        return preds;
    }

    public static void main(String[] args) {
        DgraphService ds = new DgraphService("localhost:9080");
        ds.queryRdf();
        ds.predicates();
    }

    public void add(Node s, Node p, Node o) {
        try {
            Transaction trans = client.newTransaction();
            String xidQuery = Gql.render(Gql.load("gql/xid.gql"), "subject", cleanString(s.toString()));
            String setQuery = Gql.render(Gql.load("gql/setWithXid.gql"),
                    "subject", Gql.wellFormatValue(s.toString()),
                    "predict", Gql.wellFormatPredict(p.toString()),
                    "object", Gql.wellFormatValue(o.toString()));

            trans.doRequest(DgraphProto.Request.newBuilder()
                    .setCommitNow(true)
                    .setQuery(xidQuery)
                    .addMutations(DgraphProto.Mutation.newBuilder()
                            .setSetNquads(ByteString.copyFromUtf8(setQuery))
                            .build())
                    .build());
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    public Iterator<Triple> find(Node s, Node p, Node o) {
        if (s == null) {
            return findNull(p, o);
        }
        if (s == Node.ANY) {
            return findAny(p, o);
        } else {
            return findSome(s, p, o);
        }
    }

    private Iterator<Triple> findNull(Node p, Node o) {
        if (p == null) {
            return Iter.nullIter();
        }
        if (o == null) {
            return findPred(p);
        }
        String gql = Gql.render(Gql.load("gql/findNull.gql"),
                "pred", Gql.wellFormatPredict(p.toString()),
                "val", Gql.wellFormatValue(o.toString())
        );
        logger.info("{}", gql);
        Transaction rot = client.newReadOnlyTransaction();
        DgraphProto.Response resp = rot.queryRDF(gql);
        String rdfs = resp.getRdf().toStringUtf8();
        logger.info("{}", rdfs);
        return rdfStringToTriples(rdfs);
    }

    private Iterator<Triple> findPred(Node p) {
        String gql = Gql.render(Gql.load("gql/findPred.gql"),
                "pred", Gql.wellFormatPredict(p.toString())
        );
        logger.info("{}", gql);
        Transaction rot = client.newReadOnlyTransaction();
        DgraphProto.Response resp = rot.queryRDF(gql);
        String rdfs = resp.getRdf().toStringUtf8();
        logger.info("{}", rdfs);
        return rdfStringToTriples(rdfs);
    }

    private Iterator<Triple> findSome(Node s, Node p, Node o) {
        List<String> pred = predicates();
        String fields = pred.stream().collect(Collectors.joining(" \n"));
        String gql = Gql.render(Gql.load("gql/findSome.gql"),
                "preds", fields, "xid", cleanString(s.toString()));
        Transaction rot = client.newReadOnlyTransaction();
        DgraphProto.Response resp = rot.queryRDF(gql);
        String rdfs = resp.getRdf().toStringUtf8();
        return rdfStringToTriples(rdfs);
    }

    private Iterator<Triple> singleSPO(Node sub, Node pre, Node obj) {
        Triple t = new Triple(sub, pre, obj);
        ArrayList<Triple> l = new ArrayList<Triple>();
        l.add(t);
        return l.iterator();
    }

    private Iterator<Triple> findAny(Node p, Node o) {
        List<String> pred = predicates();
        String fields = pred.stream().collect(Collectors.joining(" \n"));
        String gql = Gql.render(Gql.load("gql/findAny.gql"), "preds", fields);
        Transaction rot = client.newReadOnlyTransaction();
        DgraphProto.Response resp = rot.queryRDF(gql);
        String rdfs = resp.getRdf().toStringUtf8();
//        logger.info("{}", rdfs);
        return rdfStringToTriples(rdfs);
    }

    private Iterator<Triple> rdfStringToTriples(String rdfstr) {
        if (rdfstr.isEmpty()) {
            return Iter.nullIter();
        }
        String[] rdfs = rdfstr.split("\n");
        Map<String, String> cache = new ConcurrentHashMap<>();

        Arrays.stream(rdfs).filter(line -> line.contains("<xid>")).forEach(
                line -> {
                    String[] arr = line.split("<xid>");
                    cache.put(arr[0].trim(), arr[1].replace(" .", "").trim());
                }
        );
        List<Triple> ret = new LinkedList<>();
        Arrays.stream(rdfs).filter(line -> !line.contains("<xid>")).forEach(line -> {
            String[] arr = line.split("> ");
            String sub = cleanString(cache.get(arr[0] + ">"));
            String pre = cleanString(arr[1] + ">");
            String obj = cleanString(arr[2].replace(" .", ""));
            Triple t = new Triple(
                    NodeFactory.createLiteral(sub),
                    NodeFactory.createURI(pre),
                    NodeFactory.createLiteral(obj)
            );
            ret.add(t);
        });
//        logger.info("{}", ret);
        return ret.iterator();
    }

    private String cleanString(String s) {
        if (s == null) {
            return "";
        }
        if (s.startsWith("\"") && s.endsWith("\"")) {
            return s.substring(1, s.length() - 1);
        }
        if (s.startsWith("<") && s.endsWith(">")) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    public void delete(Node s, Node p, Node o) {
        try {
            Transaction trans = client.newTransaction();
            String xidQuery = Gql.render(Gql.load("gql/xid.gql"), "subject", cleanString(s.toString()));
            String setQuery = Gql.render(Gql.load("gql/deleteWithXid.gql"),
                    "subject", Gql.wellFormatValue(s.toString()),
                    "predict", Gql.wellFormatPredict(p.toString()),
                    "object", Gql.wellFormatValue(o.toString()));

            trans.doRequest(DgraphProto.Request.newBuilder()
                    .setCommitNow(true)
                    .setQuery(xidQuery)
                    .addMutations(DgraphProto.Mutation.newBuilder()
                            // set delete
                            .setDelNquads(ByteString.copyFromUtf8(setQuery))
                            .build())
                    .build());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.toString());
        }
    }

    public int findAllSize() {
        return 0;
    }

    public boolean hasTriple(Triple t) {
        return false;
    }

    public ExtendedIterator<Node> findAllSubjects() {
        return null;
    }

    public ExtendedIterator<Node> findAllPredicates() {
        return null;
    }

    public ExtendedIterator<Node> findAllObjects() {
        return null;
    }

    public ExtendedIterator<Triple> findTriple(Triple t) {
        return null;
    }
}
