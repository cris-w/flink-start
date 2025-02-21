package com.agibot.window;

import com.agibot.Pojo.WaterSensor;
import com.agibot.function.WaterSensrMapFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

@Slf4j
public class WindowAPIDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);

        KeyedStream<WaterSensor, String> stream = source.map(new WaterSensrMapFunction()).keyBy(WaterSensor::getId);

        // 基于时间
        var window = stream.window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)));
//        SingleOutputStreamOperator<WaterSensor> op = window.reduce((value1, value2) -> {
//            System.out.println("reduce...");
//            return WaterSensor.builder()
//                    .id(value1.getId() + "_" + value2.getId())
//                    .ts(value1.getTs())
//                    .vc(value2.getVc())
//                    .build();
//        });
        var op = window.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                log.info("process key: {}", s);
                long start = context.window().getStart();
                long end = context.window().getEnd();
                String startT = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
                String endT = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");

                long count = elements.spliterator().estimateSize();
                out.collect("key: " + s + ", start: " + startT + ", end: " + endT + ", count: " + count);
            }
        });
        op.print();
        env.execute();
    }
}
