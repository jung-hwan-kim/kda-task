package jungfly.kda.task;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

abstract public class AbstractEnricher extends RichFlatMapFunction<byte[], String> {
    protected transient MapState<String, byte[]> staged;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<String, byte[]> descriptor = new MapStateDescriptor<>(
                "Staged",
                TypeInformation.of(new TypeHint<String>() {}),
                TypeInformation.of(new TypeHint<byte[]>() {}));
        staged = getRuntimeContext().getMapState(descriptor);
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
