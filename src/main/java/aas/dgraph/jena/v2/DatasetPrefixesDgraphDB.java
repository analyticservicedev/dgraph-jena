package aas.dgraph.jena.v2;

import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.tdb.store.DatasetPrefixStorage;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DatasetPrefixesDgraphDB implements DatasetPrefixStorage {
    private PrefixMap prefixMap = PrefixMapFactory.create();

    @Override
    public Set<String> graphNames() {
        Set<String> s = new HashSet<>();
        s.add("default");
        return s;
    }

    @Override
    public String readPrefix(String s, String s1) {
        return prefixMap.get(s + s1);
    }

    @Override
    public String readByURI(String s, String s1) {
        return prefixMap.get(s + s1);
    }

    @Override
    public Map<String, String> readPrefixMap(String s) {
        return prefixMap.getMapping();
    }

    @Override
    public void insertPrefix(String s, String s1, String s2) {
        prefixMap.add(s + s1, s2);
    }

    @Override
    public void removeFromPrefixMap(String s, String s1) {
        prefixMap.delete(s + s1);
    }

    @Override
    public void removeAllFromPrefixMap(String s) {
        prefixMap.clear();
    }

    @Override
    public PrefixMap getPrefixMap() {
        return prefixMap;
    }

    @Override
    public PrefixMap getPrefixMap(String s) {
        return prefixMap;
    }

    @Override
    public void close() {

    }

    @Override
    public void sync() {

    }
}
