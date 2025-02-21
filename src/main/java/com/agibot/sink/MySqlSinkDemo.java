package com.agibot.sink;

import com.agibot.Pojo.WaterSensor;
import com.agibot.function.WaterSensrMapFunction;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class MySqlSinkDemo {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);

        SinkFunction<WaterSensor> sink = JdbcSink.sink("INSERT INTO `ws`(`id`, `ts`, `vc`) VALUES(?, ?, ?)", (JdbcStatementBuilder<WaterSensor>) (preparedStatement, waterSensor) -> {
                    preparedStatement.setString(1, waterSensor.getId());
                    preparedStatement.setLong(2, waterSensor.getTs());
                    preparedStatement.setInt(3, waterSensor.getVc());
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(100)
                        .withBatchIntervalMs(3000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl("jdbc:mysql://localhost:3306/test?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf-8&autoReconnect=true")
                        .withUsername("root")
                        .withPassword("example_root_password")
                        .withConnectionCheckTimeoutSeconds(30)
                        .build()
        );
        source.map(new WaterSensrMapFunction())
                .addSink(sink);
        env.execute();
    }
}
