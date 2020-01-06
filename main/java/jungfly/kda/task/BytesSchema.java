package jungfly.kda.task;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class BytesSchema implements SerializationSchema<byte[]>, DeserializationSchema<byte[]> {
    @Override
    public byte[] deserialize(byte[] bytes) throws IOException {
        return bytes;
    }

    @Override
    public boolean isEndOfStream(byte[] bytes) {
        return false;
    }

    @Override
    public byte[] serialize(byte[] bytes) {
        return bytes;
    }

    @Override
    public TypeInformation<byte[]> getProducedType() {
        return TypeInformation.of(new TypeHint<byte[]>() {});
    }
}
