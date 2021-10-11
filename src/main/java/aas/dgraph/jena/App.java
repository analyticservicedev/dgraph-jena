package aas.dgraph.jena;

import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class App {
    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
        System.out.println("Hello world!");
        DatasetGraphDgraphDB dbs = DgraphDBStorageBuilder.build(Location.create("hello"));
        Dataset ds = DatasetFactory.wrap(dbs);
        FusekiServer server = FusekiServer.create()
                .add("/ds", ds)
                .build();
        server.start();
    }
}
