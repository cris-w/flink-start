package com.agibot.union;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class UnionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> s1 = env.fromData(List.of(1, 2, 3, 4, 5));
        DataStreamSource<String> s2 = env.fromData(List.of("a", "b", "c", "d", "e"));
        DataStreamSource<Integer> s3 = env.fromData(List.of(6, 7, 8, 9, 10));

        DataStream<Integer> s = s1.union(s3);
        s.connect(s2).process(new CoProcessFunction<Integer, String, String>() {
            @Override
            public void processElement1(Integer value, CoProcessFunction<Integer, String, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(value.toString());
            }

            @Override
            public void processElement2(String value, CoProcessFunction<Integer, String, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(value);
            }
        }).print();

        env.execute();
    }
}
