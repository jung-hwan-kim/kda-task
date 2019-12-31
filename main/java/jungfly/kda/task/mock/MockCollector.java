package jungfly.kda.task.mock;

import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockCollector implements Collector<String> {
    private static final Logger log = LoggerFactory.getLogger(MockCollector.class);


    @Override
    public void collect(String s) {
        log.info("collecting:" + s);
    }

    @Override
    public void close() {

    }
}
