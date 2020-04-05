package com.example.java.operator.transformation;

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

public class StreamingJobFromDataStreamToDataStream {

    private static Configuration conf;
    private static StreamExecutionEnvironment streamEnv;
    private static KeyedStream<WordCount, Tuple> keyedStream;
    private static WindowedStream<WordCount, Tuple, TimeWindow> windowedStream;

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
        windowedStream = keyedStream.timeWindow(Time.seconds(5));

        // convertDataStreamToDataStream();
        // convertDataStreamToKeyedDataStream();
        convertKeyedDataStreamToDataStream();
        convertWindowedStreamToDataStream();
        // testConnect();
        testSplitAndSelect();
    }

    /**
     * KeyedDataStream -> DataStream
     * <p>
     * Rolling Aggregation, summation from beginning
     */
    public static void convertKeyedDataStreamToDataStream() throws Exception {
        // testSumFromKeyedDataStream();
        // testReduceFromKeyedDataStream();
        // testMinFromKeyedDataStream();
        // testMinByFromKeyedDataStream();
    }

    /**
     * WindowedStream -> DataStream
     * <p>
     * print message for every window
     */
    public static void convertWindowedStreamToDataStream() throws Exception {
        // testSumFromWindowedStream();
        // testReduceFromWindowedStream();
    }

    /**
     * DataStream -> DataStream
     */
    public static void testMap() throws Exception {

    }

    /**
     * DataStream -> DataStream
     */
    public static void testFlatMap() throws Exception {

    }

    /**
     * DataStream -> DataStream
     */
    public static void testFilter() throws Exception {

    }

    /**
     * DataStream -> KeyedDataStream
     */
    public static void testKeyBy() throws Exception {

    }

    /**
     * KeyedDataStream -> DataStream
     */
    public static void testSumFromKeyedDataStream() throws Exception {
        keyedStream.sum("count")
                .print();
        streamEnv.execute("Flink Stream Java API Skeleton");

    }

    /**
     * WindowedStream -> DataStream
     */
    public static void testSumFromWindowedStream() throws Exception {
        windowedStream.sum("count")
                .print();
        streamEnv.execute("Flink Stream Java API Skeleton");
    }

    /**
     * KeyedDataStream -> DataStream
     */
    public static void testReduceFromKeyedDataStream() throws Exception {
        keyedStream.reduce((v1, v2) -> new WordCount(v1.word, v1.label, v1.count * v2.count))
                .print();
        streamEnv.execute("Flink Stream Java API Skeleton");
    }

    /**
     * WindowedStream -> DataStream
     */
    public static void testReduceFromWindowedStream() throws Exception {
        windowedStream.reduce((v1, v2) -> new WordCount(v1.word, v1.label, v1.count * v2.count))
                .print();
        streamEnv.execute("Flink Stream Java API Skeleton");
    }

    /**
     * KeyedDataStream -> DataStream
     * <p>
     * group by word, order by count, return first label for all records.
     */
    public static void testMinFromKeyedDataStream() throws Exception {
        keyedStream.min("count")
                .print();
        streamEnv.execute("Flink Stream Java API Skeleton");
    }

    /**
     * WindowedStream -> DataStream
     */
    public static void testMinFromWindowedStream() throws Exception {
        windowedStream.min("count")
                .print();
        streamEnv.execute("Flink Stream Java API Skeleton");
    }

    /**
     * KeyedDataStream -> DataStream
     * <p>
     * group by word, order by count, return label which has min count.
     */
    public static void testMinByFromKeyedDataStream() throws Exception {
        keyedStream.minBy("count")
                .print();
        streamEnv.execute("Flink Stream Java API Skeleton");
    }

    /**
     * WindowedStream -> DataStream
     */
    public static void testMinByFromWindowedStream() throws Exception {
        windowedStream.minBy("count")
                .print();
        streamEnv.execute("Flink Stream Java API Skeleton");
    }

    /**
     * KeyedDataStream -> DataStream
     */
    public static void testMaxFromKeyedDataStream() throws Exception {

    }

    /**
     * WindowedStream -> DataStream
     */
    public static void testMaxFromWindowedStream() throws Exception {

    }

    /**
     * KeyedDataStream -> DataStream
     */
    public static void testMaxByFromKeyedDataStream() throws Exception {

    }

    /**
     * WindowedStream -> DataStream
     */
    public static void testMaxByFromWindowedStream() throws Exception {

    }

    /**
     * WindowedStream -> DataStream
     */
    public static void testApply() throws Exception {

    }

    /**
     * DataStream + DataStream -> ConnectedStream
     *
     * @throws Exception
     */
    public static void testConnect() throws Exception {
        DataStream<String> stringDataStream = streamEnv.fromElements("a", "b", "c", "d");
        DataStream<Integer> intDataStream = streamEnv.fromElements(1, 2, 3, 4, 5, 6);
        stringDataStream.connect(intDataStream)
                .map(new CoMapFunction<String, Integer, Object>() {
                    @Override
                    public Object map1(String value) throws Exception {
                        return "after CoFlatMap " + value;
                    }

                    @Override
                    public Object map2(Integer value) throws Exception {
                        return 10000 + value;
                    }
                })
                .print();
        streamEnv.execute("Flink Stream Java API Skeleton");
    }

    /**
     * DataStream -> SplitStream -> DataStream
     *
     * @throws Exception
     */
    public static void testSplitAndSelect() throws Exception {
        DataStream<Integer> dataStream = streamEnv.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        SplitStream<Integer> splitStream = dataStream.split(value -> {
            List<String> output = new ArrayList<>();
            if (value % 2 == 0) {
                output.add("event");
            } else {
                output.add("odd");
            }
            output.add("natural");
            return output;
        });
        DataStream<Integer> selectDataStream = splitStream.select("natural");
        selectDataStream.print();
        streamEnv.execute("Flink Stream Java API Skeleton");
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
