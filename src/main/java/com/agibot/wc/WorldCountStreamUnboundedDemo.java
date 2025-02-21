package com.agibot.wc;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * DataStream
 */
@Slf4j
public class WorldCountStreamUnboundedDemo {
    public static void main(String[] args) throws Exception {
        // 1. 执行环境
        Configuration conf = new Configuration();
        try (var env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)) {
            // 2. 读取数据 socket
            env.socketTextStream("localhost", 7777)
                    .flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    })
                    .returns(Types.TUPLE(Types.STRING, Types.INT))
                    .keyBy(value -> value.f0)
                    .sum(1)
                    .print();
            env.execute();
        }
    }
}
