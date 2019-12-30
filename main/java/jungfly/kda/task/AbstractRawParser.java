package jungfly.kda.task;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

abstract public class AbstractRawParser extends ProcessFunction<String, RawEvent> {
    protected final OutputTag<RawEvent> outputTag = new OutputTag<RawEvent>("side-output"){};

}
