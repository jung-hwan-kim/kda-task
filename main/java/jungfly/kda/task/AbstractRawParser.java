package jungfly.kda.task;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

abstract public class AbstractRawParser extends ProcessFunction<String, byte[]> {
    public final OutputTag<byte[]> ruleTag = new OutputTag<byte[]>("rule-tag"){};


    public final OutputTag<String> errorTag = new OutputTag<String>("error-tag"){};


}
