package jungfly.kda.task.mock;

import org.apache.flink.api.common.state.ValueState;

import java.io.IOException;

public class MockValueState implements ValueState<byte[]> {
    private byte[] value;
    @Override
    public byte[] value() throws IOException {
        return value;
    }

    @Override
    public void update(byte[] bytes) throws IOException {
        this.value = bytes;
    }

    @Override
    public void clear() {
        this.value = null;
    }
}
