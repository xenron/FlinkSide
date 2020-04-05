package com.example.java.operator.transformation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class StreamingJobFromWindowedStreamToDataStream {

    private static Configuration conf;
    private static StreamExecutionEnvironment streamEnv;
    private static WindowedStream<WordCount, Tuple, TimeWindow> windowedStream;

    /**
     * WindowedStream -> DataStream
     * <p>
     * process data for every window
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
        windowedStream = streamEnv
                .fromElements(
                        new WordCount("apache", "stream", 5),
                        new WordCount("apache", "batch", 10),
                        new WordCount("apache", "graph", 3),
                        new WordCount("spark", "stream", 2),
                        new WordCount("spark", "sql", 1),
                        new WordCount("spark", "graph", 4)
                )
                .keyBy("word")
                .timeWindow(Time.seconds(5));

         sumFromWindowedStream();
         reduceFromWindowedStream();
    }

    /**
     * WindowedStream -> DataStream
     */
    public static void sumFromWindowedStream() throws Exception {
        windowedStream.sum("count")
                .print();
        streamEnv.execute("Flink Stream Java API Skeleton");
    }

    /**
     * WindowedStream -> DataStream
     */
    public static void reduceFromWindowedStream() throws Exception {
        windowedStream.reduce((v1, v2) -> new WordCount(v1.word, v1.label, v1.count * v2.count))
                .print();
        streamEnv.execute("Flink Stream Java API Skeleton");
    }

    /**
     * WindowedStream -> DataStream
     */
    public static void minFromWindowedStream() throws Exception {
        windowedStream.min("count")
                .print();
        streamEnv.execute("Flink Stream Java API Skeleton");
    }

    /**
     * WindowedStream -> DataStream
     */
    public static void minByFromWindowedStream() throws Exception {
        windowedStream.minBy("count")
                .print();
        streamEnv.execute("Flink Stream Java API Skeleton");
    }

    /**
     * WindowedStream -> DataStream
     */
    public static void maxFromWindowedStream() throws Exception {

    }

    /**
     * WindowedStream -> DataStream
     */
    public static void maxByFromWindowedStream() throws Exception {

    }

    /**
     * WindowedStream -> DataStream
     */
    public static void testApply() throws Exception {

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
