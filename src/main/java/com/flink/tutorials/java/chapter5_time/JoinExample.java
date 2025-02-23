package com.flink.tutorials.java.chapter5_time;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import java.time.Duration;

public class JoinExample {
    // 在 192.168.86.135 运行：
    // nc -lk 9876
    // nc -lk 9875
    public static final String[] SVR1 = {"192.168.86.135", "9876"};
    public static final String[] SVR2 = {"192.168.86.135", "9875"};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 使用EventTime时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<String> socketSource1 = env.socketTextStream(JoinExample.SVR1[0], Integer.parseInt(JoinExample.SVR1[1]));
        DataStream<String> socketSource2 = env.socketTextStream(JoinExample.SVR1[0], Integer.parseInt(JoinExample.SVR1[1]));

        DataStream<Tuple2<String, Integer>> input1 = socketSource1.map(
                        line -> {
                            System.out.println("input1:" + line);
                            String[] arr = line.split(" +");
                            String id = arr[0];
                            int t = Integer.parseInt(arr[1]);
                            return Tuple2.of(id, t);
                        })
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        DataStream<Tuple2<String, Integer>> input2 = socketSource2.map(
                        line -> {
                            System.out.println("input2:" + line);
                            String[] arr = line.split(" +");
                            String id = arr[0];
                            int t = Integer.parseInt(arr[1]);
                            return Tuple2.of(id, t);
                        })
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        input1.print();
        input2.print();

        DataStream<String> joinResult = input1.join(input2)
                .where(i1 -> i1.f0)
                .equalTo(i2 -> i2.f0)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .apply(new MyJoinFunction());

        joinResult.print();

        env.execute("window join function");
    }

    public static class MyJoinFunction implements JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String> {

        @Override
        public String join(Tuple2<String, Integer> input1, Tuple2<String, Integer> input2) {
            return "input 1 :" + input1.f1 + ", input 2 :" + input2.f1;
        }
    }
}
