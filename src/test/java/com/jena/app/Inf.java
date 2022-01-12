package com.jena.app;

import aas.dgraph.jena.StorageDgraphDB;
import aas.dgraph.jena.store.DgraphQuadTable;
import aas.dgraph.jena.store.DgraphTripleTable;
import aas.dgraph.jena.v2.DatasetPrefixesDgraphDB;
import aas.dgraph.jena.v2.DgraphDB;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.dboe.transaction.txn.TransactionCoordinator;
import org.apache.jena.dboe.transaction.txn.TransactionalBase;
import org.apache.jena.dboe.transaction.txn.TransactionalSystem;
import org.apache.jena.dboe.transaction.txn.journal.Journal;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.*;
import org.apache.jena.reasoner.InfGraph;
import org.apache.jena.reasoner.rulesys.GenericRuleReasoner;
import org.apache.jena.reasoner.rulesys.GenericRuleReasonerFactory;
import org.apache.jena.reasoner.rulesys.Rule;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.util.PrintUtil;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import java.util.Iterator;

public class Inf {

    public static void main(String[] args) {
        System.out.println("Hello world!");
        Journal journal = Journal.create(Location.create("dgraph"));
        TransactionCoordinator txnCoord = new TransactionCoordinator(journal);
        TransactionalSystem txnSystem = new TransactionalBase(txnCoord);
        String dgraphEndpoint = "localhost:9080";
        DgraphQuadTable quadTable = new DgraphQuadTable(dgraphEndpoint);
        DgraphTripleTable tripleTable = new DgraphTripleTable(dgraphEndpoint);
        StorageDgraphDB dsg = new StorageDgraphDB(txnSystem, tripleTable, quadTable);
        DgraphDB db = new DgraphDB(dsg, new DatasetPrefixesDgraphDB());
        Dataset dataset = DatasetFactory.wrap(db);
        Model myMod = dataset.getDefaultModel();
        String finance = "http://www.example.org/kse/finance#";
        Resource 孙宏斌 = myMod.createResource(finance + "孙宏斌");
        Resource 融创中国 = myMod.createResource(finance + "融创中国");
        Resource 乐视网 = myMod.createResource(finance + "乐视网");
        Property 执掌 = myMod.createProperty(finance + "执掌");
        Resource 贾跃亭 = myMod.createResource(finance + "贾跃亭");
        Resource 地产公司 = myMod.createResource(finance + "地产公司");
        Resource 公司 = myMod.createResource(finance + "公司");
        Resource 法人实体 = myMod.createResource(finance + "法人实体");
        Resource 人 = myMod.createResource(finance + "人");
        Property 主要收入 = myMod.createProperty(finance + "主要收入");
        Resource 地产事业 = myMod.createResource(finance + "地产事业");
        Resource 王健林 = myMod.createResource(finance + "王健林");
        Resource 万达集团 = myMod.createResource(finance + "万达集团");
        Property 主要资产 = myMod.createProperty(finance + "主要资产");


        Property 股东 = myMod.createProperty(finance + "股东");
        Property 关联交易 = myMod.createProperty(finance + "关联交易");
        Property 收购 = myMod.createProperty(finance + "收购");

        // 加入三元组
        myMod.add(孙宏斌, 执掌, 融创中国);
        myMod.add(贾跃亭, 执掌, 乐视网);
        myMod.add(王健林, 执掌, 万达集团);
        myMod.add(乐视网, RDF.type, 公司);
        myMod.add(万达集团, RDF.type, 公司);
        myMod.add(融创中国, RDF.type, 地产公司);
        myMod.add(地产公司, RDFS.subClassOf, 公司);
        myMod.add(公司, RDFS.subClassOf, 法人实体);
        myMod.add(孙宏斌, RDF.type, 人);
        myMod.add(贾跃亭, RDF.type, 人);
        myMod.add(王健林, RDF.type, 人);
        myMod.add(万达集团, 主要资产, 地产事业);
        myMod.add(融创中国, 主要收入, 地产事业);
        myMod.add(孙宏斌, 股东, 乐视网);
        myMod.add(孙宏斌, 收购, 万达集团);

        PrintUtil.registerPrefix("", finance);

        // 输出当前模型
        StmtIterator i = myMod.listStatements(null, null, (RDFNode) null);
        while (i.hasNext()) {
            System.out.println(PrintUtil.print(i.nextStatement()));
        }


        GenericRuleReasoner reasoner = (GenericRuleReasoner) GenericRuleReasonerFactory.theInstance().create(null);
        reasoner.setRules(Rule.parseRules(
                "[ruleHoldShare: (?p :执掌 ?c) -> (?p :股东 ?c)] \n"
                        + "[ruleConnTrans: (?p :收购 ?c) -> (?p :股东 ?c)] \n"
                        + "[ruleConnTrans: (?p :股东 ?c) (?p :股东 ?c2) -> (?c :关联交易 ?c2)] \n"
                        + "-> tableAll()."));
        reasoner.setMode(GenericRuleReasoner.HYBRID);
        Graph g = myMod.getGraph();
        InfGraph infgraph = reasoner.bind(g);
        infgraph.setDerivationLogging(true);

        System.out.println("推理后...\n");

        Iterator<Triple> tripleIterator = infgraph.find(null, null, null);
        while (tripleIterator.hasNext()) {
            System.out.println(" - " + PrintUtil.print(tripleIterator.next()));
        }

        Dataset ds = DatasetFactory.create(myMod);
        ds.asDatasetGraph().setDefaultGraph(infgraph);
        FusekiServer server = FusekiServer.create()
                .add("/ds", ds)
                .port(6384)
                .build();
        server.start();
    }

    public static void tdbmain(String[] args) {
        System.out.println("Hello world!");
        String directory = "MyDatabases/Dataset1";
        Dataset dataset = TDBFactory.createDataset(directory);
        Model myMod = dataset.getDefaultModel();
        String finance = "http://www.example.org/kse/finance#";
        Resource 孙宏斌 = myMod.createResource(finance + "孙宏斌");
        Resource 融创中国 = myMod.createResource(finance + "融创中国");
        Resource 乐视网 = myMod.createResource(finance + "乐视网");
        Property 执掌 = myMod.createProperty(finance + "执掌");
        Resource 贾跃亭 = myMod.createResource(finance + "贾跃亭");
        Resource 地产公司 = myMod.createResource(finance + "地产公司");
        Resource 公司 = myMod.createResource(finance + "公司");
        Resource 法人实体 = myMod.createResource(finance + "法人实体");
        Resource 人 = myMod.createResource(finance + "人");
        Property 主要收入 = myMod.createProperty(finance + "主要收入");
        Resource 地产事业 = myMod.createResource(finance + "地产事业");
        Resource 王健林 = myMod.createResource(finance + "王健林");
        Resource 万达集团 = myMod.createResource(finance + "万达集团");
        Property 主要资产 = myMod.createProperty(finance + "主要资产");


        Property 股东 = myMod.createProperty(finance + "股东");
        Property 关联交易 = myMod.createProperty(finance + "关联交易");
        Property 收购 = myMod.createProperty(finance + "收购");

        // 加入三元组
        myMod.add(孙宏斌, 执掌, 融创中国);
        myMod.add(贾跃亭, 执掌, 乐视网);
        myMod.add(王健林, 执掌, 万达集团);
        myMod.add(乐视网, RDF.type, 公司);
        myMod.add(万达集团, RDF.type, 公司);
        myMod.add(融创中国, RDF.type, 地产公司);
        myMod.add(地产公司, RDFS.subClassOf, 公司);
        myMod.add(公司, RDFS.subClassOf, 法人实体);
        myMod.add(孙宏斌, RDF.type, 人);
        myMod.add(贾跃亭, RDF.type, 人);
        myMod.add(王健林, RDF.type, 人);
        myMod.add(万达集团, 主要资产, 地产事业);
        myMod.add(融创中国, 主要收入, 地产事业);
        myMod.add(孙宏斌, 股东, 乐视网);
        myMod.add(孙宏斌, 收购, 万达集团);

        PrintUtil.registerPrefix("", finance);

        // 输出当前模型
        StmtIterator i = myMod.listStatements(null, null, (RDFNode) null);
        while (i.hasNext()) {
            System.out.println(PrintUtil.print(i.nextStatement()));
        }


        GenericRuleReasoner reasoner = (GenericRuleReasoner) GenericRuleReasonerFactory.theInstance().create(null);
        reasoner.setRules(Rule.parseRules(
                "[ruleHoldShare: (?p :执掌 ?c) -> (?p :股东 ?c)] \n"
                        + "[ruleConnTrans: (?p :收购 ?c) -> (?p :股东 ?c)] \n"
                        + "[ruleConnTrans: (?p :股东 ?c) (?p :股东 ?c2) -> (?c :关联交易 ?c2)] \n"
                        + "-> tableAll()."));
        reasoner.setMode(GenericRuleReasoner.HYBRID);

        InfGraph infgraph = reasoner.bind(myMod.getGraph());
        infgraph.setDerivationLogging(true);

        System.out.println("推理后...\n");

        Iterator<Triple> tripleIterator = infgraph.find(null, null, null);
        while (tripleIterator.hasNext()) {
            System.out.println(" - " + PrintUtil.print(tripleIterator.next()));
        }

        Dataset ds = DatasetFactory.create(myMod);
        ds.asDatasetGraph().setDefaultGraph(infgraph);
        FusekiServer server = FusekiServer.create()
                .add("/ds", ds)
                .port(6384)
                .build();
        server.start();
    }

    public static void memoryMain(String[] args) {
        System.out.println("Hello world!");
        Model myMod = ModelFactory.createDefaultModel();
        String finance = "http://www.example.org/kse/finance#";
        Resource 孙宏斌 = myMod.createResource(finance + "孙宏斌");
        Resource 融创中国 = myMod.createResource(finance + "融创中国");
        Resource 乐视网 = myMod.createResource(finance + "乐视网");
        Property 执掌 = myMod.createProperty(finance + "执掌");
        Resource 贾跃亭 = myMod.createResource(finance + "贾跃亭");
        Resource 地产公司 = myMod.createResource(finance + "地产公司");
        Resource 公司 = myMod.createResource(finance + "公司");
        Resource 法人实体 = myMod.createResource(finance + "法人实体");
        Resource 人 = myMod.createResource(finance + "人");
        Property 主要收入 = myMod.createProperty(finance + "主要收入");
        Resource 地产事业 = myMod.createResource(finance + "地产事业");
        Resource 王健林 = myMod.createResource(finance + "王健林");
        Resource 万达集团 = myMod.createResource(finance + "万达集团");
        Property 主要资产 = myMod.createProperty(finance + "主要资产");


        Property 股东 = myMod.createProperty(finance + "股东");
        Property 关联交易 = myMod.createProperty(finance + "关联交易");
        Property 收购 = myMod.createProperty(finance + "收购");

        // 加入三元组
        myMod.add(孙宏斌, 执掌, 融创中国);
        myMod.add(贾跃亭, 执掌, 乐视网);
        myMod.add(王健林, 执掌, 万达集团);
        myMod.add(乐视网, RDF.type, 公司);
        myMod.add(万达集团, RDF.type, 公司);
        myMod.add(融创中国, RDF.type, 地产公司);
        myMod.add(地产公司, RDFS.subClassOf, 公司);
        myMod.add(公司, RDFS.subClassOf, 法人实体);
        myMod.add(孙宏斌, RDF.type, 人);
        myMod.add(贾跃亭, RDF.type, 人);
        myMod.add(王健林, RDF.type, 人);
        myMod.add(万达集团, 主要资产, 地产事业);
        myMod.add(融创中国, 主要收入, 地产事业);
        myMod.add(孙宏斌, 股东, 乐视网);
        myMod.add(孙宏斌, 收购, 万达集团);

        PrintUtil.registerPrefix("", finance);

        // 输出当前模型
        StmtIterator i = myMod.listStatements(null, null, (RDFNode) null);
        while (i.hasNext()) {
            System.out.println(PrintUtil.print(i.nextStatement()));
        }


        GenericRuleReasoner reasoner = (GenericRuleReasoner) GenericRuleReasonerFactory.theInstance().create(null);
        reasoner.setRules(Rule.parseRules(
                "[ruleHoldShare: (?p :执掌 ?c) -> (?p :股东 ?c)] \n"
                        + "[ruleConnTrans: (?p :收购 ?c) -> (?p :股东 ?c)] \n"
                        + "[ruleConnTrans: (?p :股东 ?c) (?p :股东 ?c2) -> (?c :关联交易 ?c2)] \n"
                        + "-> tableAll()."));
        reasoner.setMode(GenericRuleReasoner.HYBRID);

        InfGraph infgraph = reasoner.bind(myMod.getGraph());
        infgraph.setDerivationLogging(true);

        System.out.println("推理后...\n");

        Iterator<Triple> tripleIterator = infgraph.find(null, null, null);
        while (tripleIterator.hasNext()) {
            System.out.println(" - " + PrintUtil.print(tripleIterator.next()));
        }

        Dataset ds = DatasetFactory.create(myMod);
        ds.asDatasetGraph().setDefaultGraph(infgraph);
        FusekiServer server = FusekiServer.create()
                .add("/ds", ds)
                .port(6384)
                .build();
        server.start();
    }
}
