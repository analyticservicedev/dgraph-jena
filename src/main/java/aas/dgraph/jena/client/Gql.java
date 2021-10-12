package aas.dgraph.jena.client;

import org.apache.commons.io.IOUtils;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import freemarker.cache.StringTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import static freemarker.template.Configuration.DEFAULT_INCOMPATIBLE_IMPROVEMENTS;

public abstract class Gql {
    public static String load(String name) {
        File f = new File(name);
        Resource init = null;
        if (f.exists() && f.isFile()) {
            init = new FileSystemResource(f);
        } else {
            init = new ClassPathResource(name);
        }
        try {
            return IOUtils.toString(init.getInputStream(), "utf8");
        } catch (IOException e) {
        }
        return "";
    }

    /**
     * @param tpl  模板.
     * @param data data.
     * @return string.
     */
    public static String render(final String tpl, final Map<String, ?> data) {
        StringWriter out = new StringWriter();
        Template x = null;
        try {
            x = t(tpl);
            x.process(data, out);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return out.toString();
    }

    private static Template t(String tpl) throws Exception {
        Configuration conf = new Configuration(
                DEFAULT_INCOMPATIBLE_IMPROVEMENTS);
        StringTemplateLoader renderer = new StringTemplateLoader();
        renderer.putTemplate("tpl", tpl);
        conf.setTemplateLoader(renderer);
        conf.setClassicCompatible(true);
        return conf.getTemplate("tpl");
    }

    /**
     * @param tpl     　模板.
     * @param keyVals k-v pair.
     * @return 字符串.
     */
    public static String render(final String tpl, final String... keyVals) {
        Map<String, Object> x = new HashMap<>();
        for (int i = 0; i < keyVals.length; i = i + 2) {
            x.put(keyVals[i], keyVals[i + 1]);
        }
        return render(tpl, x);
    }
}
