package com.flink.tutorials.java.chapter5_time;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Arrays;
import java.util.Random;

/**
 * 演示 Flink 算子间数据传输的 8 种策略：
 * <pre>
 *
 * 1) forward：在上下游并行度相同的情况下，按照 subtaskIndex 一对一传输。
 * 2) shuffle：随机分发数据，常用于打散数据。
 * 3) rebalance：轮询的方式分发数据，常用于负载均衡。
 * 4) rescale：在本分区内轮询到下游的所有并行子任务，但它只会在 “本分区子任务” 与 “下游子任务” 间做轮询。
 * 5) broadcast：将同一份数据发送到所有下游 并行子任务。
 * 6) global：将所有数据都发送到下游的第 0 号并行子任务，类似收敛到一个子任务处理。
 * 7) union：将多个相同类型的 DataStream 合并成一个流（多路合并，数据类型需相同）。
 * 8) keyBy：按照 key 做哈希分区，用于分组聚合或有状态算子等。
 *
 * 场景示例：
 * 我们先定义一个传感器数据 SensorReading(id, timestamp, temperature)。
 * - 从一组模拟数据构建第一个主流 sensorStream。
 * - 依次演示各分区策略。每个分区操作后都可能 再返回 DataStream 给下一步。
 * - 最终，将结果输出时可以区分 不同阶段，观察各转换策略 的效果（在并行度>1场景下更明显）。
 *
 * 注意事项：
 * - forward、shuffle、rebalance、rescale、global 等只能在转换时调用，例如 sensorStream.forward()。
 * - keyBy、broadcast、union 则是对应的专用API或方法。
 *
 * 注意：如需同时让传感器数据流与配置流都变成 BroadcastStream，是无法再直接 connect(...) 的；
 *      常见的正确用法是将“配置信息流”设为 BroadcastStream，而“主数据流”保持为普通 DataStream，
 *      然后调用主数据流的 .connect(广播流)，才能获得 BroadcastConnectedStream，避免类型冲突。
 * </pre>
 */
public class PartitionStrategiesDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        Configuration conf = new Configuration();
        // For Flink Web UI, open the link in your browser http://localhost:8082
        conf.set(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);

        // 2. 构建源: 无限传感器数据流 (普通 DataStream)
        DataStream<SensorReading> sensorStream = env.addSource(new MockSensorSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                                    @Override
                                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                                        // 返回事件时间戳（毫秒）
                                        return element.timestamp;
                                    }
                                })
                );

        // 3) 展示各种分区策略
        // 3.1 shuffle：随机分发
        DataStream<SensorReading> shuffleStream = sensorStream
                .shuffle()
                .map(new TagMapFunction("After-Shuffle"))
                .name("1-shuffle-map");

        // 3.2 forward：需要上下游并行度一致才能生效，否则自动退化为 shuffle
        //     这里下游我们改为并行度2，对比上游并行度4，不完全匹配
        //     所以 forward 不会真的生效，演示时可自行调整
        DataStream<SensorReading> forwardStream = shuffleStream
                .forward()
                .map(new TagMapFunction("After-Forward"))
                .setParallelism(4)
                .name("2-forward-map");

        // 3.3 rebalance：按 round-robin(轮询) 将数据分发，常用于负载均衡
        DataStream<SensorReading> rebalanceStream = forwardStream
                .rebalance()
                .map(new TagMapFunction("After-Rebalance"))
                .name("3-rebalance-map");

        // 3.4 rescale：在本分区内轮询到下游并行子任务
        DataStream<SensorReading> rescaleStream = rebalanceStream
                .rescale()
                .map(new TagMapFunction("After-Rescale"))
                .name("4-rescale-map");

        // 4) 演示 broadcast + connect：
        //    通常只广播“配置流”，主数据流保持普通 DataStream，即可 connect() 得到 BroadcastConnectedStream。
        //
        //    先创建一个普通“配置流”，再对其调用 broadcast()，得到一个 BroadcastStream<String>。
        DataStream<String> configStream = env.fromData(
                Arrays.asList("CONFIG_A=1", "CONFIG_B=2")
        );

        // 为了使用 BroadcastProcessFunction，需要一个 StateDescriptor
        MapStateDescriptor<String, String> configDescriptor =
                new MapStateDescriptor<>("configState", String.class, String.class);

        // 广播配置流
        BroadcastStream<String> broadcastConfigStream = configStream.broadcast(configDescriptor);

        // 连接 (connect) 普通 DataStream<SensorReading> 与广播流 BroadcastStream<String>
        BroadcastConnectedStream<SensorReading, String> connectedBroadcast =
                rescaleStream.connect(broadcastConfigStream);

        // 利用 BroadcastProcessFunction 同时处理主数据 & 广播数据
        DataStream<SensorReading> processedBroadcast = connectedBroadcast.process(new MyBroadcastProcessFunction());

        // 5) global：将所有数据都发送到下游第 0 号并行子任务
        DataStream<SensorReading> globalStream = processedBroadcast
                .global()
                .map(new TagMapFunction("After-Global"))
                .name("5-global-map");

        // 6) union：把多个相同类型的 DataStream 合并为一个
        DataStream<SensorReading> extraStream = env.fromData(
                new SensorReading("sensor_X", System.currentTimeMillis() + new Random().nextInt(10000), 99.9),
                new SensorReading("sensor_Y", System.currentTimeMillis() + new Random().nextInt(10000), 88.8)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                            @Override
                            public long extractTimestamp(SensorReading element, long recordTimestamp) {
                                // 返回事件时间戳（毫秒）
                                return element.timestamp;
                            }
                        })
        );

        DataStream<SensorReading> unionStream = globalStream
                .union(extraStream)
                .map(new TagMapFunction("After-Union"))
                .name("6-union-map");

        // 7) keyBy：对相同 key 做哈希分区，一般用于聚合或有状态函数
        //    这里示例对同一 sensorId 的温度进行简单 average
        SingleOutputStreamOperator<Tuple2<String, Double>> keyedResult = unionStream
                .keyBy((KeySelector<SensorReading, String>) value -> value.id)
                .timeWindow(Time.seconds(10))
                .reduce((s1, s2) -> {
                    // 取时间戳为较大的那个，并把温度做平均
                    return new SensorReading(
                            s1.id,
                            Math.max(s1.timestamp, s2.timestamp),
                            (s1.temperature + s2.temperature) / 2
                    );
                })
                .map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(SensorReading value) {
                        return Tuple2.of(value.id, value.temperature);
                    }
                });

        // 输出结果
        keyedResult.print("final-result");

        env.execute("Flink Partition Strategies Demo (Fix Broadcast Connect)");
    }

    // 简单的传感器数据类
    public static class SensorReading {
        public String id;
        public long timestamp;
        public double temperature;

        public SensorReading() {
        }

        public SensorReading(String id, long timestamp, double temperature) {
            this.id = id;
            this.timestamp = timestamp;
            this.temperature = temperature;
        }

        @Override
        public String toString() {
            return "SensorReading{" +
                    "id='" + id + '\'' +
                    ", timestamp=" + timestamp +
                    ", temperature=" + temperature +
                    '}';
        }
    }

    /**
     * 自定义的无限数据源，模拟随机的传感器温度数据
     */
    public static class MockSensorSource implements SourceFunction<SensorReading> {

        private volatile boolean running = true;
        private final Random random = new Random();

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            // 准备一些传感器ID，以便生成数据
            String[] sensorIds = {
                    "sensor_1", "sensor_2", "sensor_3",
                    "sensor_4", "sensor_5", "sensor_6",
                    "sensor_7", "sensor_8"
            };
            while (running) {
                // 随机从这些ID里挑一个
                String chosenId = sensorIds[random.nextInt(sensorIds.length)];
                // 使用系统当前时间作为 timestamp
                long currentTs = System.currentTimeMillis();
                // 模拟温度在 20 ~ 40 范围内浮动
                double temp = 20 + (20 * random.nextDouble());

                // 发送到下游
                ctx.collect(new SensorReading(chosenId, currentTs, temp));
                // 控制一下速率
                Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /**
     * 演示 BroadcastProcessFunction：处理两个输入流:
     * - 主数据流 (非广播)
     * - 配置广播流 (Broadcast)
     */
    public static class MyBroadcastProcessFunction
            extends BroadcastProcessFunction<SensorReading, String, SensorReading> {

        // 维护一个 BroadcastState 描述符，用于存储配置
        private final MapStateDescriptor<String, String> configStateDescriptor =
                new MapStateDescriptor<>("configState", String.class, String.class);

        @Override
        public void processElement(SensorReading value, ReadOnlyContext ctx, Collector<SensorReading> out) throws Exception {
            // 在这里可以读取 configState 中的配置，基于配置对数据做丰富或过滤
            // 演示简单，直接往下游发送
            out.collect(value);
        }

        @Override
        public void processBroadcastElement(String configValue, Context ctx, Collector<SensorReading> out) throws Exception {
            BroadcastState<String, String> broadcastState = ctx.getBroadcastState(configStateDescriptor);
            // 例如处理形如 "CONFIG_A=1"
            String[] kv = configValue.split("=");
            if (kv.length == 2) {
                broadcastState.put(kv[0], kv[1]);
            }
        }
    }

    /**
     * 给每条记录打上一个标签 (仅用于演示分区策略时的日志打印)。
     */
    public static class TagMapFunction extends RichMapFunction<SensorReading, SensorReading> {

        private final String tag;

        public TagMapFunction(String tag) {
            this.tag = tag;
        }

        @Override
        public SensorReading map(SensorReading value) {
            System.out.println("[" + tag + "] SubtaskIndex="
                    + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()
                    + " => " + value.toString());
            return value;
        }
    }
}
