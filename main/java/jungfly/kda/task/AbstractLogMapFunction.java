package jungfly.kda.task;

import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractLogMapFunction implements MapFunction<RawEvent, String> {
    private static final Logger log = LoggerFactory.getLogger(AbstractLogMapFunction.class);

    protected String name;

    public AbstractLogMapFunction name(String name) {
        this.name = name;
        return this;
    }

    @Override
    public String map(RawEvent rawEvent) throws Exception {
        return name + "> type=" + rawEvent.getType() + ",id=" + rawEvent.getId() + ",op=" + rawEvent.getOp();
    }
}
