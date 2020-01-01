package jungfly.kda.task;


import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

abstract public class AbstractBroadcaster extends BroadcastProcessFunction<byte[], byte[], byte[]> {
    public final MapStateDescriptor<String, byte[]> ruleStateDescriptor =
            new MapStateDescriptor<>(
                    "RulesBroadcastState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(new TypeHint<byte[]>() {}));
    private static final Logger log = LoggerFactory.getLogger(AbstractBroadcaster.class);

    abstract public void process(byte[] smile, Iterable<Map.Entry<String, byte[]>> stateIterable, Collector<byte[]> collector) throws Exception;

    @Override
    public void processElement(byte[] rawEvent, ReadOnlyContext readOnlyContext, Collector<byte[]> collector) throws Exception {
        process(rawEvent, readOnlyContext.getBroadcastState(ruleStateDescriptor).immutableEntries(), collector);
    }



    abstract public void processBroadcast(byte[] smile, BroadcastState<String, byte[]> state, Collector<byte[]> collector) throws Exception;

    @Override
    public void processBroadcastElement(byte[] rawEvent, Context context, Collector<byte[]> collector) throws Exception {
        processBroadcast(rawEvent, context.getBroadcastState(ruleStateDescriptor), collector);
    }
}
