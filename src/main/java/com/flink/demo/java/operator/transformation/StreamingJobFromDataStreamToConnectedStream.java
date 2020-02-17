package com.flink.demo.java.operator.transformation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class StreamingJobFromDataStreamToConnectedStream {

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
        connectStream();
    }

    /**
     * DataStream + DataStream -> ConnectedStream
     *
     * @throws Exception
     */
    public static void connectStream() throws Exception {
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

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class WordCount {
        private String word;
        private String label;
        private int count;
    }

}
