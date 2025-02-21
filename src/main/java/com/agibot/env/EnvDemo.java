package com.agibot.env;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EnvDemo {
    public static void main(String[] args) throws Exception {
        var conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "8082");

        var env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
    }
}
