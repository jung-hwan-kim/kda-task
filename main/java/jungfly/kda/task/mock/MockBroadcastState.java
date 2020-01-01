package jungfly.kda.task.mock;

import org.apache.flink.api.common.state.BroadcastState;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MockBroadcastState implements BroadcastState<String, byte[]> {
    private Map<String, byte[]> map = new HashMap<>();

    @Override
    public void put(String s, byte[] bytes) throws Exception {
        map.put(s, bytes);
    }

    @Override
    public void putAll(Map<String, byte[]> m) throws Exception {
        map.putAll(m);
    }

    @Override
    public void remove(String s) throws Exception {
        map.remove(s);
    }

    @Override
    public Iterator<Map.Entry<String, byte[]>> iterator() throws Exception {
        return map.entrySet().iterator();
    }
    private <T> Iterable<T> getIterableFromIterator(Iterator<T> iterator) {
        return () -> iterator;
    }

    @Override
    public Iterable<Map.Entry<String, byte[]>> entries() throws Exception {
        return getIterableFromIterator(map.entrySet().iterator());
    }

    @Override
    public byte[] get(String s) throws Exception {
        return map.get(s);
    }

    @Override
    public boolean contains(String s) throws Exception {
        return map.containsKey(s);
    }

    @Override
    public Iterable<Map.Entry<String, byte[]>> immutableEntries() throws Exception {
        return entries();
    }

    @Override
    public void clear() {
        map.clear();
    }
}
