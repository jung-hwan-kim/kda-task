package jungfly.kda.task;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

abstract public class AbstractKeyedBroadcaster extends KeyedBroadcastProcessFunction<String, byte[], byte[], byte[]> {


    public final MapStateDescriptor<String, byte[]> bstateDescriptor =
            new MapStateDescriptor<>(
                    "BroadcastState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(new TypeHint<byte[]>() {}));

    public final ValueStateDescriptor<byte[]> kstateDescriptor =
            new ValueStateDescriptor<>(
                    "KeyState",
                    TypeInformation.of(new TypeHint<byte[]>() {}));

    private transient ValueState<byte[]> kstate;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        kstate = getRuntimeContext().getState(kstateDescriptor);
    }


    private static final Logger log = LoggerFactory.getLogger(AbstractKeyedBroadcaster.class);

    abstract public void process(byte[] smile, ValueState<byte[]> kstate, Iterable<Map.Entry<String, byte[]>> pstateIterable, Collector<byte[]> collector) throws Exception;

    @Override
    public void processElement(byte[] rawEvent, ReadOnlyContext readOnlyContext, Collector<byte[]> collector) throws Exception {
        process(rawEvent, kstate, readOnlyContext.getBroadcastState(bstateDescriptor).immutableEntries(), collector);
    }

    abstract public void processBroadcast(byte[] smile, BroadcastState<String, byte[]> state, Collector<byte[]> collector) throws Exception;

    @Override
    public void processBroadcastElement(byte[] rawEvent, Context context, Collector<byte[]> collector) throws Exception {
        processBroadcast(rawEvent, context.getBroadcastState(bstateDescriptor), collector);
    }
}
