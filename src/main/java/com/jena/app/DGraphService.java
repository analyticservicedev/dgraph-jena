package com.jena.app;

public class DGraphService {
    private DGraph dGraph;
    private String host;
    private Integer port;

    public DGraphService(DGraph dGraph) {
        this.dGraph = dGraph;
        this.host = dGraph.getHost();
        this.port = dGraph.getPort();
    }
}
