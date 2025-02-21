package com.agibot.window;

import com.agibot.Pojo.WaterSensor;
import com.agibot.function.WaterSensrMapFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

@Slf4j
public class AggregateAndProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);
        KeyedStream<WaterSensor, String> stream = source.map(new WaterSensrMapFunction()).keyBy(WaterSensor::getId);

        var window = stream.window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)));

        SingleOutputStreamOperator<String> op = window.aggregate(new MyAgg(), new MyProcess());
        op.print();

        env.execute();
    }


    public static class MyAgg implements AggregateFunction<WaterSensor, Integer, String> {
        @Override
        public Integer createAccumulator() {
            log.info("create accumulator...");
            return 0;
        }

        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            log.info("add value: {}", value);
            return accumulator + value.getVc();
        }

        @Override
        public String getResult(Integer accumulator) {
            log.info("get result accumulator: {}", accumulator);
            return "count -> " + accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    public static class MyProcess extends ProcessWindowFunction<String, String, String, TimeWindow> {
        @Override
        public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
            log.info("process key: {}", s);
            long start = context.window().getStart();
            long end = context.window().getEnd();
            String startT = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
            String endT = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");

            long count = elements.spliterator().estimateSize();
            out.collect("key: " + s + ", start: " + startT + ", end: " + endT + ", count: " + count + ", element: " + elements);
        }
    }
}
