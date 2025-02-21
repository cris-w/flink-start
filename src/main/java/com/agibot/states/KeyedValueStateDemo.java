package com.agibot.states;

import com.agibot.Pojo.WaterSensor;
import com.agibot.function.WaterSensrMapFunction;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 值状态
 */
public class KeyedValueStateDemo {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<WaterSensor> op = source.map(new WaterSensrMapFunction())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((element, _) -> element.getTs() * 1000L)
                );

        op.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    ValueState<Integer> lastVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 初始化状态
                        lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastVcState", Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        // 取出上一条水印
                        Integer old = lastVcState.value();
                        // 求差值绝对值是否超过10
                        Integer vc = value.getVc();
                        if (old != null && Math.abs(vc - old) > 10) {
                            out.collect(String.format("传感器id = %s，当前水印%d, 上一条水印%d, 超过阈值10", value.getId(), vc, old));
                        }
                        // 保存新的水印
                        lastVcState.update(vc);
                    }
                }).print();
        env.execute();
    }
}
