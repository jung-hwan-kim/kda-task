package jungfly.kda.task;

import org.apache.flink.api.common.functions.MapFunction;

public interface AbstractParser extends MapFunction<String, byte[]> {
}