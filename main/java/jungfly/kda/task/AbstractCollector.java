package jungfly.kda.task;

import org.apache.flink.api.common.functions.MapFunction;

public interface AbstractCollector extends MapFunction<String, String> {
}
