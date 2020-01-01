package jungfly.kda.task;

import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractLogMapFunction implements MapFunction<byte[], String> {
    private static final Logger log = LoggerFactory.getLogger(AbstractLogMapFunction.class);
    protected String name;
    public AbstractLogMapFunction name(String name) {
        this.name = name;
        return this;
    }
}
