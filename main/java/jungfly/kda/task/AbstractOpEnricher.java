package jungfly.kda.task;

import clojure.lang.PersistentArrayMap;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public abstract class AbstractOpEnricher implements FlatMapFunction<byte[], String>, CheckpointedFunction {
    private transient ListState<Tuple2<String, byte[]>> checkpointedState;
    private static final Logger log = LoggerFactory.getLogger(AbstractOpEnricher.class);

    protected Map<String, PersistentArrayMap> staged;
    public AbstractOpEnricher() {
        staged = new HashMap<>();

    }

    abstract public byte[] serialize(PersistentArrayMap m);

    abstract public PersistentArrayMap deserialize(byte[] b);

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        long current = System.currentTimeMillis();
        long timestamp = functionSnapshotContext.getCheckpointTimestamp();
        checkpointedState.clear();
        for (Map.Entry<String, PersistentArrayMap> entry : staged.entrySet()) {
            Tuple2<String, byte[]> a = new Tuple2<>(entry.getKey(), serialize(entry.getValue()));
            checkpointedState.add(a);
        }
        log.info("snapshot-state: " + (current - timestamp));
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        log.info("initialize-state");
        ListStateDescriptor<Tuple2<String, byte[]>> descriptor = new ListStateDescriptor<>(
                "stage", TypeInformation.of(new TypeHint<Tuple2<String, byte[]>>() {}));
        checkpointedState = functionInitializationContext.getOperatorStateStore().getUnionListState(descriptor);
        if (functionInitializationContext.isRestored()) {
            for (Tuple2<String, byte[]> element : checkpointedState.get()) {
                staged.put(element._1, deserialize(element._2));
            }
        }
    }
}
