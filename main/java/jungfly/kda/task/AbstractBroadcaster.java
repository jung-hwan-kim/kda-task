package jungfly.kda.task;


import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

abstract public class AbstractBroadcaster extends BroadcastProcessFunction<RawEvent, RawEvent, RawEvent> {
    public final MapStateDescriptor<String, RawEvent> ruleStateDescriptor =
            new MapStateDescriptor<>(
                    "RulesBroadcastState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(new TypeHint<RawEvent>() {}));
    private static final Logger log = LoggerFactory.getLogger(AbstractBroadcaster.class);


    @Override
    public void processElement(RawEvent rawEvent, ReadOnlyContext readOnlyContext, Collector<RawEvent> collector) throws Exception {

        log.info("PROC:" + rawEvent.getType() + ":" + rawEvent.getId() + ":" + rawEvent.getOp());
        for (Map.Entry<String, RawEvent> e : readOnlyContext.getBroadcastState(ruleStateDescriptor).immutableEntries()) {
            log.info(" *entry* " + e.getKey() + ": " + e.getValue().getType());
        }
        collector.collect(rawEvent);
    }

    @Override
    public void processBroadcastElement(RawEvent rawEvent, Context context, Collector<RawEvent> collector) throws Exception {
        log.info("BROADCAST:" + rawEvent.getType() + ":" + rawEvent.getId() + ":" + rawEvent.getOp());
        context.getBroadcastState(ruleStateDescriptor).get(rawEvent.getId());
        context.getBroadcastState(ruleStateDescriptor).put(rawEvent.getId(), rawEvent);
    }
}
