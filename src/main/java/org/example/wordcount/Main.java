package org.example.wordcount;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class Main {
    
    public static void main(String[] args) throws Exception {
        // 设置Flink执行环境
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT, 8082);
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, config);
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-group");
        String inputTopic = "Shakespeare";
        //Source
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>(inputTopic, new SimpleStringSchema(), properties);
        DataStreamSource<String> stream = env.addSource(consumer);
        
        // Transformation
        DataStream<Tuple2<String, Integer>> wordCount = stream
                .flatMap((String line, Collector<Tuple2<String, Integer>> collector) -> {
                    String[] tokens = line.split("\\s");
                    for (String token : tokens) {
                        if (token.length() > 0) {
                            collector.collect(new Tuple2<>(token, 1));
                        }
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);
        
        //Sink
        wordCount.print();
        
        //execution
        env.execute("kafka streaming word count");
    }
}
