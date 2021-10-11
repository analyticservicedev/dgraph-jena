package com.jena.app;

import org.apache.jena.graph.impl.TripleStore;
import org.apache.jena.mem.GraphMem;

public class DGraph extends GraphMem {

    private String host;
    private Integer port;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public DGraph() {
        super();
    }

    @Override
    protected TripleStore createTripleStore() {
        return new DGraphTripleStore(this);
    }
}
