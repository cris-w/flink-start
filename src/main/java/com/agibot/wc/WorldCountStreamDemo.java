package com.agibot.wc;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * DataStream
 */
@Slf4j
public class WorldCountStreamDemo {
    public static void main(String[] args) throws Exception {
        // 1. 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据 ("src/main/resources/hello.txt");
        FileSource<String> fileSource = FileSource.
                forRecordStreamFormat(new TextLineInputFormat(), new Path("src/main/resources/hello.txt"))
                .build();
        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "FileSource")
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
