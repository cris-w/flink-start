package com.agibot.states;

import com.agibot.Pojo.WaterSensor;
import com.agibot.function.WaterSensrMapFunction;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 值状态
 */
public class KeyedListStateDemo {
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
                .process(new KeyedProcessFunction<String, WaterSensor, List<Integer>>() {

                    ListState<Integer> vcListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        vcListState = getRuntimeContext().getListState(new ListStateDescriptor<>("vcListState", Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<  String, WaterSensor, List<Integer>>.Context ctx, Collector<List<Integer>> out) throws Exception {
                        vcListState.add(value.getVc());
                        List<Integer> top3 = new ArrayList<>(3);
                        for (Integer vc : vcListState.get()) {
                           // 保证top3
                            if (top3.size() < 3) {
                                top3.add(vc);
                            } else {
                                top3.sort((o1, o2) -> o2 - o1);
                                if (vc > top3.get(2)) {
                                    top3.set(2, vc);
                                }
                            }
                        }
                        out.collect(top3);
                        vcListState.update(top3);
                    }
                }).print();
        env.execute();
    }
}
