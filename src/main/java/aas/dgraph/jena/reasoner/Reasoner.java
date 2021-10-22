package aas.dgraph.jena.reasoner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
public class Reasoner {
    private final static Logger log = LoggerFactory.getLogger(Reasoner.class);
    private String rules;

    @PostConstruct
    void loadRulesFromDisk() {
        log.info("load rules from disk !");
    }

    public String getRules() {
        return rules;
    }
}
