package jungfly.kda.task;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

abstract public class AbstractKeyedBroadcaster extends KeyedBroadcastProcessFunction<String, byte[], byte[], byte[]> {
    public final OutputTag<String> sideTag = new OutputTag<String>("side-tag"){};

    public final StateTtlConfig ttlConfig = StateTtlConfig
            .newBuilder(Time.days(1))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
            .cleanupInBackground()
            .build();

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

    public AbstractKeyedBroadcaster() {
        kstateDescriptor.enableTimeToLive(ttlConfig);
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        kstate = getRuntimeContext().getState(kstateDescriptor);
    }


    private static final Logger log = LoggerFactory.getLogger(AbstractKeyedBroadcaster.class);

    abstract public String process(byte[] smile, ValueState<byte[]> kstate, Iterable<Map.Entry<String, byte[]>> pstateIterable, Collector<byte[]> collector) throws Exception;


    @Override
    public void processElement(byte[] rawEvent, ReadOnlyContext readOnlyContext, Collector<byte[]> collector) throws Exception {

        String result = process(rawEvent, kstate, readOnlyContext.getBroadcastState(bstateDescriptor).immutableEntries(), collector);
        if (result != null) {
            readOnlyContext.output(sideTag, result);
        }
    }

    abstract public String processBroadcast(byte[] smile, BroadcastState<String, byte[]> state, Collector<byte[]> collector) throws Exception;

    @Override
    public void processBroadcastElement(byte[] rawEvent, Context context, Collector<byte[]> collector) throws Exception {
        String result = processBroadcast(rawEvent, context.getBroadcastState(bstateDescriptor), collector);
        if (result != null) {
            context.output(sideTag, result);
        }
    }
}
