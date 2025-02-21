package com.agibot.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGeneratorDemo {
    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        //  设置并行度
        env.setParallelism(1);
        var source = new DataGeneratorSource<>(index -> "Number: " + index, 10, RateLimiterStrategy.perSecond(1), Types.STRING);
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Generator Source")
                .print();
        env.execute();
    }
}
