package com.agibot.process;

import com.agibot.Pojo.WaterSensor;
import com.agibot.function.WaterSensrMapFunction;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

@Slf4j
public class KeyedProcessTimerDemo {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<WaterSensor> op = source.map(new WaterSensrMapFunction());

        WatermarkStrategy<WaterSensor> strategy = WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((element, _) -> element.getTs() * 1000)
                .withIdleness(Duration.ofSeconds(1));
        op.assignTimestampsAndWatermarks(strategy).keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) {
                        TimerService timer = ctx.timerService();
                        timer.registerEventTimeTimer(5000);
                        log.info("key = {},当前时间是 {}, 注册了一个5s的计时器", ctx.getCurrentKey(), ctx.timestamp());
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        log.info("key = {}, 定时器触发时间是 {}", ctx.getCurrentKey(), timestamp);
                    }
                }).print();

        env.execute();
    }
}
