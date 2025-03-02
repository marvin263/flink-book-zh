package com.flink.tutorials.java.chapter5_time;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Collections;

public class StudentScoreAverage {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        Configuration conf = new Configuration();
        // http://localhost:8082/#/overview
        conf.set(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // 2. 连接 Socket。通过 nc -lk 9876 命令来模拟输入
        DataStream<String> input = env.socketTextStream(JoinExample.SVR1[0], Integer.parseInt(JoinExample.SVR1[1]));

        // 3. 接收输入并解析
        //    假设每行格式： Tom 82
        //    其中 Tom 表示学生 ID，82 表示该学生某次成绩
        DataStream<Tuple2<String, Integer>> studentScores = input.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                String[] fields = value.split("\\s+");
                if (fields.length == 2) {
                    try {
                        String studentId = fields[0];
                        int score = Integer.parseInt(fields[1]);
                        out.collect(Tuple2.of(studentId, score));
                    } catch (NumberFormatException e) {
                        // 忽略异常或根据需求处理数据格式不正确的情况
                    }
                } else {
                    System.out.println("Invalid input");
                }
            }
        });

        // 4. 按学生 ID 分组，并基于时间窗口（例如 5 分钟）计算平均值
        //    这里演示了两种方法：
        //    方法1：自定义 WindowFunction，手动计算 sum 和 count 再求平均
        //    方法2：利用富函数 (RichMapFunction) 或 AggregateFunction
        //    以下代码使用 WindowFunction 做示例

        studentScores
                // 按 studentId 分组
                .keyBy(value -> value.f0)
                // 设置滚动窗口时长为 5 分钟
                .window(new MyTumblingWindowAssigner(5 * 60 * 1000L))
                .apply(new MyWindowFunction())
                .print();

        // 5. 启动流式作业
        env.execute("Flink KeyBy and Average Score");
    }

    public static class MyWindowFunction implements WindowFunction<Tuple2<String, Integer>, Tuple2<String, Double>, String, TimeWindow> {
        @Override
        public void apply(String key,
                          TimeWindow window,
                          Iterable<Tuple2<String, Integer>> inputData,
                          Collector<Tuple2<String, Double>> out) {

            int sum = 0;
            int count = 0;
            for (Tuple2<String, Integer> record : inputData) {
                sum += record.f1;
                count++;
            }
            double average = (count == 0) ? 0 : (double) sum / count;
            // 输出 (学生ID, 平均值)
            out.collect(Tuple2.of(key, average));
        }
    }

    public static class MyTumblingWindowAssigner extends WindowAssigner<Object, TimeWindow> {
        private final long windowSize; // 窗口长度，毫秒级

        public MyTumblingWindowAssigner(long windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
            // 获取当前处理时间
            long currentProcessingTime = context.getCurrentProcessingTime();
            // 计算窗口起始和结束
            long start = currentProcessingTime - (currentProcessingTime % windowSize);
            long end = start + windowSize;
            // 将此元素放入 [start, end) 的窗口中
            return Collections.singletonList(new TimeWindow(start, end));
        }

        @Override
        public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
            // 使用处理时间触发器
            return ProcessingTimeTrigger.create();
        }

        @Override
        public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
            return new TimeWindow.Serializer();
        }

        @Override
        public boolean isEventTime() {
            return false;  // 表示基于处理时间
        }
    }
}
