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
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.ArrayList;
import java.util.List;

public class StreamingJobFromDataStreamToSplitStream {

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

        testSplitAndSelect();
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
