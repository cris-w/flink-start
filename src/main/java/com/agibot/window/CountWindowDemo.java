package com.agibot.window;

import com.agibot.Pojo.WaterSensor;
import com.agibot.function.WaterSensrMapFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

@Slf4j
public class CountWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);

        KeyedStream<WaterSensor, String> stream = source.map(new WaterSensrMapFunction()).keyBy(WaterSensor::getId);

        // 基于数量
        WindowedStream<WaterSensor, String, GlobalWindow> window = stream.countWindow(5, 2);
        window.process(new ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                out.collect("key: " + s + " count => " + elements.spliterator().estimateSize());
            }
        }).print();
        env.execute();
    }
}
