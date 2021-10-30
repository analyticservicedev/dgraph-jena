package aas.dgraph.jena.v2;

import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.tdb.store.DatasetPrefixStorage;

import java.util.Map;
import java.util.Set;

public class DatasetPrefixesDgraphDB  implements DatasetPrefixStorage {
    @Override
    public Set<String> graphNames() {
        return null;
    }

    @Override
    public String readPrefix(String s, String s1) {
        return null;
    }

    @Override
    public String readByURI(String s, String s1) {
        return null;
    }

    @Override
    public Map<String, String> readPrefixMap(String s) {
        return null;
    }

    @Override
    public void insertPrefix(String s, String s1, String s2) {

    }

    @Override
    public void removeFromPrefixMap(String s, String s1) {

    }

    @Override
    public void removeAllFromPrefixMap(String s) {

    }

    @Override
    public PrefixMap getPrefixMap() {
        return null;
    }

    @Override
    public PrefixMap getPrefixMap(String s) {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void sync() {

    }
}
