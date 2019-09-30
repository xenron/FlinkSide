package com.flink.demo.java.transformation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.ArrayList;
import java.util.List;

public class StreamingJobFromKeyedDataStreamToDataStream {

    private static Configuration conf;
    private static StreamExecutionEnvironment streamEnv;
    private static KeyedStream<WordCount, Tuple> keyedStream;

    /**
     * KeyedDataStream -> DataStream
     * <p>
     * Rolling Aggregation, process data for every message
     */
    public static void main(String[] args) throws Exception {

        // http://localhost:9191/
        // http://localhost:9191/#/overview
        conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        streamEnv.setParallelism(3).setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        keyedStream = streamEnv
                .fromElements(
                        new WordCount("apache", "stream", 5),
                        new WordCount("apache", "batch", 10),
                        new WordCount("apache", "graph", 3),
                        new WordCount("spark", "stream", 2),
                        new WordCount("spark", "sql", 1),
                        new WordCount("spark", "graph", 4)
                )
                .keyBy("word");
        sumFromKeyedDataStream();
        reduceFromKeyedDataStream();
        minFromKeyedDataStream();
        minByFromKeyedDataStream();
    }

    /**
     * KeyedDataStream -> DataStream
     */
    public static void sumFromKeyedDataStream() throws Exception {
        keyedStream.sum("count")
                .print();
        streamEnv.execute("Flink Stream Java API Skeleton");

    }

    /**
     * KeyedDataStream -> DataStream
     */
    public static void reduceFromKeyedDataStream() throws Exception {
        keyedStream.reduce((v1, v2) -> new WordCount(v1.word, v1.label, v1.count * v2.count))
                .print();
        streamEnv.execute("Flink Stream Java API Skeleton");
    }

    /**
     * KeyedDataStream -> DataStream
     * <p>
     * group by word, order by count, return first label for all records.
     */
    public static void minFromKeyedDataStream() throws Exception {
        keyedStream.min("count")
                .print();
        streamEnv.execute("Flink Stream Java API Skeleton");
    }

    /**
     * KeyedDataStream -> DataStream
     * <p>
     * group by word, order by count, return label which has min count.
     */
    public static void minByFromKeyedDataStream() throws Exception {
        keyedStream.minBy("count")
                .print();
        streamEnv.execute("Flink Stream Java API Skeleton");
    }

    /**
     * KeyedDataStream -> DataStream
     */
    public static void maxFromKeyedDataStream() throws Exception {

    }

    /**
     * KeyedDataStream -> DataStream
     */
    public static void maxByFromKeyedDataStream() throws Exception {

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class WordCount {
        private String word;
        private String label;
        private int count;
    }

}
