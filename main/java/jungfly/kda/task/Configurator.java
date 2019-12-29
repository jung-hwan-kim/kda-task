package jungfly.kda.task;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class Configurator {
    private static final String REGION = "us-east-1";
    private static final String INPUT_STREAM_NAME = "ds-prototype-raw";
    private static final String OUTPUT_STREAM_NAME = "ds-prototype-out";

    private static final Logger log = LoggerFactory.getLogger(Configurator.class);

    public static FlinkKinesisConsumer<String> createSource() throws IOException {
        Properties consumerConfig = getConsumerConfig();
        String name = consumerConfig.getProperty("stream.name");
        return new FlinkKinesisConsumer<>(name, new SimpleStringSchema(), consumerConfig);
    }

    public static FlinkKinesisProducer<String> createSink() throws IOException {
        Properties producerConfig = getProducerConfig();
        String name = producerConfig.getProperty("stream.name");
        String defaultPartition = producerConfig.getProperty("default.partition");
        FlinkKinesisProducer<String> sink = new FlinkKinesisProducer<>(new SimpleStringSchema(), producerConfig);
        sink.setDefaultStream(name);
        sink.setDefaultPartition(defaultPartition);
        return sink;
    }

    private static Properties getConsumerConfig() throws IOException {
        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        Properties consumerConfig = applicationProperties.get("ConsumerConfigProperties");
        log.info("ConsumerConfig:" + consumerConfig);
        if (consumerConfig == null) {
            log.info("Configuring from default");
            consumerConfig = new Properties();
            consumerConfig.setProperty(ConsumerConfigConstants.AWS_REGION, REGION);
            consumerConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");
            consumerConfig.setProperty("stream.name", INPUT_STREAM_NAME);
        }
        return consumerConfig;
    }
    private static Properties getProducerConfig() throws IOException {
        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        Properties producerConfig = applicationProperties.get("ProducerConfigProperties");
        log.info("ProducerConfig:" + producerConfig);
        if (producerConfig == null) {
            log.info("Configuring from default");
            producerConfig = new Properties();
            producerConfig.setProperty(ConsumerConfigConstants.AWS_REGION, REGION);
            producerConfig.setProperty("AggregationEnabled", "false");
            producerConfig.setProperty("stream.name", OUTPUT_STREAM_NAME);
            producerConfig.setProperty("default.partition", "0");
        }
        return producerConfig;
    }

    public static void run() throws Exception {
       final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       env.getStateBackend();
//        DataStreamSource<String> in = env.addSource(createSource());
//        in.name("rrr");
////        SingleOutputStreamOperator<String> stream = in.flatMap(new ));
//        stream.name("stream");
//        DataStreamSink<String> out = stream.addSink(createSink());
//        out.name("out");

//        DataStream<String> input = env.addSource(createSource()).name("raw");
//        input.map(new LogProcess()).name("log")
//                .addSink(createSink()).name("out");
//        env.execute("KDA Prototype");
    }


}
