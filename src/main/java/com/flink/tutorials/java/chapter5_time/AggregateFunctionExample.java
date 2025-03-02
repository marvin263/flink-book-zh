package com.flink.tutorials.java.chapter5_time;

import com.flink.tutorials.java.utils.stock.StockPrice;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class AggregateFunctionExample {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        Configuration conf = new Configuration();
        // For Flink Web UI, open the link in your broswer http://localhost:8082
        conf.set(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        // 建议在高并发场景下根据实际情况配置并行度
        // env.setParallelism(2);

        // 2. 构建 FileSource，用于读取本地或 HDFS 上的 CSV 文件
        Path path = new Path(
                AggregateFunctionExample.class
                        .getResource("/stock/stock-tick-20200108.csv") // 注意前面加"/"
                        .toURI()
        );
        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineFormat(), path)
                .build();

        // 3. 从 FileSource 读取字符串流，并在后续步骤中进行水印分配
        DataStream<String> lines = env.fromSource(
                fileSource,
                // 占位，先不分配水印，等解析为 StockPrice 后再做水印分配
                WatermarkStrategy.noWatermarks(),
                "StockSource"
        );

        // 4. 将每行字符串映射为 StockPrice 对象
        DataStream<StockPrice> stockStream = lines.map(new MapFunction<String, StockPrice>() {
                    private long lastEventTs = 0;

                    @Override
                    public StockPrice map(String line) throws Exception {
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd HHmmss");
                        String[] itemStrArr = line.split(",");
                        LocalDateTime dateTime = LocalDateTime.parse(itemStrArr[StockPrice.IDX_YYYYMMDD] + " " + itemStrArr[StockPrice.IDX_HHMMSS], formatter);
                        long currentEventTs = Timestamp.valueOf(dateTime).getTime();
                        StockPrice stock = StockPrice.of(itemStrArr[StockPrice.IDX_SYMBOL],
                                Double.parseDouble(itemStrArr[StockPrice.IDX_PRICE]),
                                currentEventTs,
                                Integer.parseInt(itemStrArr[StockPrice.IDX_VOLUME]));
                        // 输入文件中的时间戳是从小到大排列的
                        // 新读入的行如果比上一行大，sleep，这样来模拟一个有时间间隔的输入流
                        if (currentEventTs - lastEventTs > 0L) {
                            Thread.sleep(Math.min((currentEventTs - lastEventTs), 100L));
                        }
                        lastEventTs = currentEventTs;
                        return stock;
                    }
                })
                // 5. 分配水印和事件时间戳，这里假设数据的 timestamp 字段已经是毫秒级的事件时间
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<StockPrice>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<StockPrice>() {
                                    @Override
                                    public long extractTimestamp(StockPrice element, long recordTimestamp) {
                                        // 返回事件时间戳（毫秒）
                                        return element.ts;
                                    }
                                })
                );

        // 6. 在事件时间语义下对相同 symbol 的股票数据进行10秒滚动窗口聚合，例如计算平均价格
        DataStream<Tuple2<String, Double>> average = stockStream
                .keyBy(s -> s.symbol)
                // 使用事件时间 tumbling 窗口
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .aggregate(new AverageAggregate());

        // 7. 打印结果
        average.print();

        // 8. 启动执行
        env.execute("window aggregate function with WatermarkStrategy");
    }

    /**
     * 接收三个泛型：
     * IN: StockPrice
     * ACC：(String, Double, Int) - (symbol, sum, count)
     * OUT: (String, Double) - (symbol, average)
     */
    public static class AverageAggregate implements AggregateFunction<StockPrice, Tuple3<String, Double, Integer>, Tuple2<String, Double>> {

        @Override
        public Tuple3<String, Double, Integer> createAccumulator() {
            return Tuple3.of("", 0d, 0);
        }

        @Override
        public Tuple3<String, Double, Integer> add(StockPrice item, Tuple3<String, Double, Integer> accumulator) {
            double price = accumulator.f1 + item.price;
            int count = accumulator.f2 + 1;
            return Tuple3.of(item.symbol, price, count);
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> accumulator) {
            return Tuple2.of(accumulator.f0, accumulator.f1 / accumulator.f2);
        }

        @Override
        public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> a, Tuple3<String, Double, Integer> b) {
            return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }
}
