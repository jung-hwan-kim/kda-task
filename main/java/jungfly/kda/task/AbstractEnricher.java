package jungfly.kda.task;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.configuration.Configuration;

abstract public class AbstractEnricher extends RichFlatMapFunction<byte[], String> {
    protected transient MapState<String, String> staged;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
//        ValueStateDescriptor<ObjectNode> descriptor = new ValueStateDescriptor<>(
//                "Staged", TypeInformation.of(new TypeHint<ObjectNode>() {})
//        );
//        staged = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
