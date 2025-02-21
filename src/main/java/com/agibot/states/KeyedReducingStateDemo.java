package com.agibot.states;

import com.agibot.Pojo.WaterSensor;
import com.agibot.function.WaterSensrMapFunction;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 值状态
 */
public class KeyedReducingStateDemo {
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
                    ReducingState<Integer> vcReducingState;

                    @Override
                    public void open(OpenContext openContext) throws Exception {
                        super.open(openContext);
                        vcReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<>("vcReducingState", Integer::sum, Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                        Integer vc = value.getVc();
                        vcReducingState.add(vc);

                        out.collect("key: " + value.getId() + " vc: " + vcReducingState.get());
                    }

                }).print();
        env.execute();
    }
}
