package jungfly.kda.task;


import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class AbstractBroadcaster extends BroadcastProcessFunction<RawEvent, RawEvent, RawEvent> {

    @Override
    public void processElement(RawEvent rawEvent, ReadOnlyContext readOnlyContext, Collector<RawEvent> collector) throws Exception {

    }

    @Override
    public void processBroadcastElement(RawEvent rawEvent, Context context, Collector<RawEvent> collector) throws Exception {

    }
}
