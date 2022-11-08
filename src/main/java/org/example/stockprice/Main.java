package org.example.stockprice;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT, 8082);
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, config);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        
        StockSource source = new StockSource("stock/stock-tick-20200108.csv");
        DataStreamSource<StockPrice> stream = env.addSource(source);
        
        stream.keyBy("symbol")
                .max("price")
                .print();
    
        env.execute("stock price");
    }
}
