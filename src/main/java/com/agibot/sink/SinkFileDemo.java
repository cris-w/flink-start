package com.agibot.sink;

import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

public class SinkFileDemo {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度2
        env.setParallelism(2);

        // 开启检查点
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

        DataGeneratorSource<String> source = new DataGeneratorSource<>(index -> "Number: " + index, 1000, RateLimiterStrategy.perSecond(10), Types.STRING);
        DataStreamSource<String> dataGen = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Generator Source");

        FileSink<String> fileSink = FileSink.<String>forRowFormat(new Path("./tmp"), new SimpleStringEncoder<>())
                // 输出文件配置
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("agibot")
                        .withPartSuffix(".log")
                        .build())
                // 文件按目录分桶
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH", ZoneId.of("UTC")))
                // 文件滚动策略 10S 或 1KB
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(Duration.ofSeconds(10))
                        .withMaxPartSize(MemorySize.parse("1 KB"))
                        .build())
                .build();
        dataGen.sinkTo(fileSink);

        env.execute();
    }
}
