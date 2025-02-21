package com.agibot.source;

import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CollectionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FileSource<String> fileSource = FileSource.
                forRecordStreamFormat(new TextLineInputFormat(), new Path("src/main/resources/hello.txt"))
                .build();

        env.execute();
    }
}
