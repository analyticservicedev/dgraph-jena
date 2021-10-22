package aas.dgraph.jena.reasoner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.PostConstruct;

@Controller
public class ReasonerController {
    private final static Logger log = LoggerFactory.getLogger(ReasonerController.class);
    private String rules;


    @RequestMapping("/add-rules")
    @ResponseBody
    public String appendRules() {
        return "add rules";
    }
}
