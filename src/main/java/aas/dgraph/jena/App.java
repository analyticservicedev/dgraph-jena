package aas.dgraph.jena;

import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.springframework.beans.factory.annotation.Value;
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
        FusekiServer server = FusekiServer.create()
                .add("/ds", ds)
                .port(6384)
                .build();
        server.start();
    }
}
