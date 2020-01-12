package jungfly.kda.task;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

public class AbstractFilter extends RichFilterFunction<byte[]> {

    protected transient ValueState<byte[]> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<byte[]> descriptor = new ValueStateDescriptor<>("state", TypeInformation.of(new TypeHint<byte[]>() {
        }));
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public boolean filter(byte[] bytes) throws Exception {
        return true;
    }
}
