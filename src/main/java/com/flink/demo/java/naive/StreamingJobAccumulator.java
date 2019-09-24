package com.flink.demo.java.naive;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

public class StreamingJobAccumulator {
    public static void main(String[] args) throws Exception {
        count();
    }

    public static void count() throws Exception {
        // http://localhost:9191/
        // http://localhost:9191/#/overview
        // Overview -> Running Jobs -> Overview -> Accumulator
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        streamEnv.setParallelism(4).setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        streamEnv
                .socketTextStream("localhost", 9999)
                .flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
                    private LongCounter lineCounter = new LongCounter(0);
                    private LongCounter wordCounter = new LongCounter(0);

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        getRuntimeContext().addAccumulator("line-number", lineCounter);
                        getRuntimeContext().addAccumulator("word-number", wordCounter);
                    }

                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        Stream.of(s.split(" "))
                                .forEach(value -> {
                                    collector.collect(Tuple2.of(value, 1));
                                    wordCounter.add(1);
                                });
                        lineCounter.add(1);
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1)
                .print();
        streamEnv.execute("Flink Stream Java API Skeleton");
    }
}
