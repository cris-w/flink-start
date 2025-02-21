package com.agibot.function;

import com.agibot.Pojo.WaterSensor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;

@NoArgsConstructor
public class WaterSensrMapFunction implements MapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String value) throws Exception {
        String[] arr = value.split(" ");
        return WaterSensor.builder()
                .id(arr[0])
                .ts(Long.valueOf(arr[1]))
                .vc(Integer.valueOf(arr[2]))
                .build();
    }
}
