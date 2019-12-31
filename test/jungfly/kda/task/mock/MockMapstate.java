package jungfly.kda.task.mock;

import org.apache.flink.api.common.state.MapState;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MockMapstate implements MapState<String, byte[]> {
    private Map<String, byte[]> map = new HashMap<>();
    public MockMapstate() {

    }
    @Override
    public byte[] get(String aString) throws Exception {
        return map.get(aString);
    }

    @Override
    public void put(String aString, byte[] b) throws Exception {
        map.put(aString, b);
    }

    @Override
    public void putAll(Map<String, byte[]> m) throws Exception {
        map.putAll(m);
    }

    @Override
    public void remove(String aString) throws Exception {
        map.remove(aString);
    }

    @Override
    public boolean contains(String aString) throws Exception {
        return map.containsKey(aString);
    }

    private <T> Iterable<T> getIterableFromIterator(Iterator<T> iterator) {
        return () -> iterator;
    }

    @Override
    public Iterable<Map.Entry<String, byte[]>> entries() throws Exception {
        return getIterableFromIterator(iterator());
    }
    @Override
    public Iterable<String> keys() throws Exception {
        return getIterableFromIterator(map.keySet().iterator());
    }

    @Override
    public Iterable<byte[]> values() throws Exception {
        return getIterableFromIterator(map.values().iterator());
    }

    @Override
    public Iterator<Map.Entry<String, byte[]>> iterator() throws Exception {
        return map.entrySet().iterator();
    }

    @Override
    public void clear() {
        map.clear();
    }
}
