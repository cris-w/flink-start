package com.agibot.watermark;

import com.agibot.Pojo.WaterSensor;
import com.agibot.function.WaterSensrMapFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

@Slf4j
public class WaterMarkMonoDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<WaterSensor> op = source.map(new WaterSensrMapFunction());
        WatermarkStrategy<WaterSensor> strategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner((element, recordTimestamp) -> {
                    log.info("element: {}, recordTimestamp: {}", element, recordTimestamp);
                    return element.getTs() * 1000;
                })
                .withIdleness(Duration.ofSeconds(1));
        op.assignTimestampsAndWatermarks(strategy).keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        out.collect("key: " + s + " count => " + elements.spliterator().estimateSize());
                    }
                }).print();

        env.execute();
    }
}
