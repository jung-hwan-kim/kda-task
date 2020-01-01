package jungfly.kda.task;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.util.OutputTag;
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
    public static void configure(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> src = env.addSource(createSource());

    }

//    public static StreamExecutionEnvironment configurePrototype03(AbstractParser parser, AbstractOpEnricher enricher) throws Exception {
//       final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//       DataStreamSource<String> in = env.addSource(createSource());
//             in.map(parser).name("parser")
//               .flatMap(enricher).name("op-enricher").setMaxParallelism(1).setParallelism(1)
//               .addSink(createSink());
//       env.enableCheckpointing(5000);
//       return env;
//    }

//    public static StreamExecutionEnvironment configurePrototype04(AbstractRawParser parser, AbstractLogMapFunction logFunction) throws Exception {
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> in = env.addSource(createSource());
//        in.map(parser).name("rawparser")
//                .map(logFunction.name("LOG")).name("log")
//                .addSink(createSink());
//        return env;
//    }
    public static StreamExecutionEnvironment configurePrototype05(AbstractRawParser parser,
                                                                  AbstractLogMapFunction logFunction,
                                                                  AbstractLogMapFunction sideLogFunction,
                                                                  AbstractLogMapFunction errorLogFunction,
                                                                  AbstractEnricher opEnricher,
                                                                  AbstractCoEnricher coEnricher) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final OutputTag<byte[]> ruleTag = new OutputTag<byte[]>("rule-tag"){};
        final OutputTag<byte[]> errorTag = new OutputTag<byte[]>("error-tag"){};
        SinkFunction<String> out = createSink();
        DataStreamSource<String> in = env.addSource(createSource());
        in.name("in");
        SingleOutputStreamOperator<byte[]> mainStream = in.process(parser).name("raw-parse");
        SingleOutputStreamOperator<byte[]> mainStream2 = mainStream.flatMap(opEnricher).name("actor");



        DataStream<byte[]> ruleStream = mainStream.getSideOutput(parser.ruleTag);
//        ruleStream.map(sideLogFunction.name("RULE")).name("rule").addSink(out).name("out");

        DataStream<byte[]> errorStream = mainStream.getSideOutput(parser.errorTag);
        errorStream.map(errorLogFunction.name("ERR")).name("err").addSink(out).name("out");

        SingleOutputStreamOperator<byte[]> enriched = mainStream2.connect(ruleStream).flatMap(coEnricher).name("rule");
        enriched.setParallelism(1);
        enriched.setMaxParallelism(1);
        enriched.map(logFunction.name("LOG")).name("log").addSink(out).name("out");
//        MapStateDescriptor<String, byte[]> ruleStateDescriptor = new MapStateDescriptor<String, byte[]>(
//                "RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<byte[]>() {}));

        //ruleStream.broadcast(ruleStateDescriptor);

        //mainStream.connect(mainStream).process();
        return env;
    }

    public static StreamExecutionEnvironment configurePrototype06(AbstractRawParser parser,
                                                                  AbstractLogMapFunction logFunction,
                                                                  AbstractLogMapFunction errorLogFunction,
                                                                  AbstractEnricher opEnricher,
                                                                  AbstractBroadcaster broadcaster) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final OutputTag<byte[]> ruleTag = new OutputTag<byte[]>("rule-tag"){};
        final OutputTag<byte[]> errorTag = new OutputTag<byte[]>("error-tag"){};
        SinkFunction<String> out = createSink();
        DataStreamSource<String> in = env.addSource(createSource());
        in.name("in");
        SingleOutputStreamOperator<byte[]> mainStream = in.process(parser).name("raw-parse");
        SingleOutputStreamOperator<byte[]> mainStream2 = mainStream.flatMap(opEnricher).name("actor");


        DataStream<byte[]> ruleStream = mainStream.getSideOutput(parser.ruleTag);
        DataStream<byte[]> errorStream = mainStream.getSideOutput(parser.errorTag);
        errorStream.map(errorLogFunction.name("ERR")).name("err").addSink(out).name("out");

//        MapStateDescriptor<String, byte[]> ruleStateDescriptor = new MapStateDescriptor<String, byte[]>(
//                "RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<byte[]>() {}));

        BroadcastStream<byte[]> broadcastStream = ruleStream.broadcast(broadcaster.ruleStateDescriptor);
        SingleOutputStreamOperator<byte[]> enriched = mainStream2.connect(broadcastStream).process(broadcaster);
        enriched.map(logFunction.name("LOG")).name("log").addSink(out).name("out");
        return env;
    }
}
