package jungfly.kda.task;

import org.apache.flink.api.java.functions.KeySelector;

public interface AbstractSelector extends KeySelector<byte[], Long> {
}
