package com.agibot.transform;

import com.agibot.Pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> source = env.fromData(List.of(new WaterSensor("s1", 1L, 1), new WaterSensor("s2", 1L, 1), new WaterSensor("s3", 1L, 1), new WaterSensor("s1", 1L, 1)));
        KeyedStream<WaterSensor, String> part = source.keyBy(WaterSensor::getId);
        part.reduce((value1, value2) -> new WaterSensor(value1.getId(), value1.getTs(), value1.getVc() + value2.getVc()))
                .print();
        env.execute();
    }
}
