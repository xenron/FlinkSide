package com.example.java.naive;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RestartStrategy {
    static StreamExecutionEnvironment streamEnv;
    public static void main(String[] args) throws Exception {
        // http://localhost:9191/
        // http://localhost:9191/#/overview
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        fixDelayRestart();
        fixFailureRestart();
    }

    public static void fixDelayRestart() throws Exception {
        streamEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
        streamEnv
                .fromCollection(IntStream.range(0, 1000000).boxed().collect(Collectors.toList()))
                .returns(Types.INT)
                .flatMap((Integer tuple, Collector<Tuple2<Integer, Integer>> out) -> {
                    IntStream.range(0, 300).boxed().forEach(num -> out.collect(Tuple2.of(num, tuple)));
                })
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .setParallelism(2)
                .keyBy(0)
                .reduce((tuple1, tuple2) -> Tuple2.of(tuple1.f0, tuple1.f1 + tuple2.f1))
                .print()
                .setParallelism(1);
        streamEnv.execute("Flink Stream Java API Skeleton");
    }


    public static void fixFailureRestart() throws Exception {
        streamEnv.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.seconds(100), Time.seconds(10)));
        streamEnv
                .fromCollection(IntStream.range(0, 1000000).boxed().collect(Collectors.toList()))
                .returns(Types.INT)
                .flatMap((Integer tuple, Collector<Tuple2<Integer, Integer>> out) -> {
                    IntStream.range(0, 300).boxed().forEach(num -> out.collect(Tuple2.of(num, tuple)));
                })
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .setParallelism(2)
                .keyBy(0)
                .reduce((tuple1, tuple2) -> Tuple2.of(tuple1.f0, tuple1.f1 + tuple2.f1))
                .print()
                .setParallelism(1);
        streamEnv.execute("Flink Stream Java API Skeleton");
    }


}
