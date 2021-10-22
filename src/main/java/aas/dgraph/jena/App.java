package aas.dgraph.jena;

import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.reasoner.InfGraph;
import org.apache.jena.reasoner.rulesys.GenericRuleReasoner;
import org.apache.jena.reasoner.rulesys.GenericRuleReasonerFactory;
import org.apache.jena.reasoner.rulesys.Rule;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.util.PrintUtil;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class App {

    public static void main(String[] args) {
        ConfigurableApplicationContext ctx = SpringApplication.run(App.class, args);
        System.out.println("Hello world!");
        String dgraph = ctx.getEnvironment().getProperty("dgraph.endpoint");
        DatasetGraphDgraphDB dbs = DgraphDBStorageBuilder.build(dgraph, Location.create("logs"));
        Dataset ds = DatasetFactory.wrap(dbs);
        Model model = ds.getDefaultModel();
        GenericRuleReasoner reasoner = (GenericRuleReasoner) GenericRuleReasonerFactory.theInstance().create(null);
        String finance = "http://www.w3.org/2001/vcard-rdf/3.0#";
        PrintUtil.registerPrefix("", finance);

        reasoner.setRules(Rule.parseRules(
                "[findFaSmith: (?p :Family Smith) -> (?p :FaSmith ?c)] "
                        + "-> tableAll()."));
        reasoner.setMode(GenericRuleReasoner.HYBRID);

        InfGraph infGraph = reasoner.bind(model.getGraph());
        infGraph.setDerivationLogging(true);
//        ds.asDatasetGraph().addGraph(Node.ANY, infGraph);
        Dataset mds = DatasetFactory.create();
        DatasetGraph mdg = mds.asDatasetGraph();
        mdg.setDefaultGraph(infGraph);
        mdg.addGraph(null,ds.asDatasetGraph().getDefaultGraph());
        FusekiServer server = FusekiServer.create()
                .add("/ds", mdg)
                .port(6384)
                .build();
        server.start();
    }
}
