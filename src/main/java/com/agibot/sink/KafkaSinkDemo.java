package com.agibot.sink;

import com.agibot.function.WaterSensrMapFunction;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaSinkDemo {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 开启检查点 （精准一次必须开启，否则无法写入）
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("192.168.31.179:9092")
                // 序列化器
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic("agibot")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                // 发送到Kafka的一致性级别：精准一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 事务ID前缀
                .setTransactionalIdPrefix("agibot-")
                // 事务超时时间: 精准一次必须设置，并且要大于检查点间隔且小于最大事务超时时间15min
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "360000")
                .build();

        // WaterSensor to json
        ObjectMapper mapper = new ObjectMapper();
        source.map(new WaterSensrMapFunction())
                .map(mapper::writeValueAsString)
                .sinkTo(sink);
        env.execute();
    }
}
