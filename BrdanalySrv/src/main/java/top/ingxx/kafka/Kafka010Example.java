package top.ingxx.kafka;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import javax.annotation.Nullable;

public class Kafka010Example {

    public static void main(String[] args) throws Exception {
        // parse input arguments
        args = new String[]{"--input-topic","test1","--output-topic","test2","--bootstrap.servers","192.168.80.134:9092","--zookeeper.connect","192.168.80.134:2181","--group.id","myconsumer"};
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 5) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-topic <topic> --output-topic <topic> " +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> --group.id <some id>");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<KafkaEvent> input = env
                .addSource(
                        new FlinkKafkaConsumer010<>(
                                parameterTool.getRequired("input-topic"),
                                new KafkaEventSchema(),
                                parameterTool.getProperties())
                                .assignTimestampsAndWatermarks(new CustomWatermarkExtractor()))
                .keyBy("word")
                .map(new RollingAdditionMapper());

        input.addSink(
                new FlinkKafkaProducer010<>(
                        parameterTool.getRequired("output-topic"),
                        new KafkaEventSchema(),
                        parameterTool.getProperties()));

        env.execute("Kafka 0.10 Example");
    }

    /**
     * A {@link RichMapFunction} that continuously outputs the current total frequency count of a key.
     * The current total count is keyed state managed by Flink.
     */
    private static class RollingAdditionMapper extends RichMapFunction<KafkaEvent, KafkaEvent> {

        private static final long serialVersionUID = 1180234853172462378L;

        private transient ValueState<Integer> currentTotalCount;

        @Override
        public KafkaEvent map(KafkaEvent event) throws Exception {
            Integer totalCount = currentTotalCount.value();
            System.out.println("哈哈--");
            if (totalCount == null) {
                totalCount = 0;
            }
            totalCount += event.getFrequency();

            currentTotalCount.update(totalCount);

            return new KafkaEvent(event.getWord(), totalCount, event.getTimestamp());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            currentTotalCount = getRuntimeContext().getState(new ValueStateDescriptor<>("currentTotalCount", Integer.class));
        }
    }

    /**
     * A custom {@link AssignerWithPeriodicWatermarks}, that simply assumes that the input stream
     * records are strictly ascending.
     *
     * <p>Flink also ships some built-in convenience assigners, such as the
     * {@link BoundedOutOfOrdernessTimestampExtractor} and {@link AscendingTimestampExtractor}
     */
    private static class CustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<KafkaEvent> {

        private static final long serialVersionUID = -742759155861320823L;

        private long currentTimestamp = Long.MIN_VALUE;

        @Override
        public long extractTimestamp(KafkaEvent event, long previousElementTimestamp) {
            // the inputs are assumed to be of format (message,timestamp)
            this.currentTimestamp = event.getTimestamp();
            return event.getTimestamp();
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
        }
    }
}


