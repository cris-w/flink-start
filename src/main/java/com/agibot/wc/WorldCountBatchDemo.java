package com.agibot.wc;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

@Slf4j
public class WorldCountBatchDemo {
    public static void main(String[] args) throws Exception {
        // 1. 执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据
        DataSource<String> data = env.readTextFile("src/main/resources/hello.txt");
        // 3. 处理数据：切分/按单词分组/聚合/输出
        data.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0)
                .sum(1)
                .print();
    }
}
