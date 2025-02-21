package com.agibot.function;

import com.agibot.Pojo.WaterSensor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;

@NoArgsConstructor
public class MyMapFunction implements MapFunction<WaterSensor, String> {

    @Override
    public String map(WaterSensor value) throws Exception {
        return value.getId();
    }
}
