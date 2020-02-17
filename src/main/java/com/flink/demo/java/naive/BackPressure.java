package com.flink.demo.java.naive;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BackPressure {
    static StreamExecutionEnvironment streamEnv;
    public static void main(String[] args) throws Exception {
        // http://localhost:9191/
        // http://localhost:9191/#/overview
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        backPressureHighForSourceAndOperator();
        backPressureHighForOperator();
    }

    public static void backPressureHighForSourceAndOperator() throws Exception {
        streamEnv
                .fromCollection(IntStream.range(0, 1000000).boxed().collect(Collectors.toList()),
                        Types.INT)
                .flatMap((Integer tuple, Collector<Integer> out) -> {
                    IntStream.range(0, 300).boxed().forEach(num -> out.collect(tuple));
                })
                .returns(Types.INT)
                .setParallelism(2)
                .print()
                .setParallelism(1);
        streamEnv.execute("Flink Stream Java API Skeleton");
    }

    public static void backPressureHighForOperator() throws Exception {
        streamEnv
                .fromCollection(IntStream.range(0, 10).boxed().collect(Collectors.toList()),
                        Types.INT)
                .flatMap((Integer tuple, Collector<Integer> out) -> {
                    IntStream.range(0, 3000000).boxed().forEach(num -> out.collect(tuple));
                })
                .returns(Types.INT)
                .setParallelism(2)
                .print()
                .setParallelism(1);
        streamEnv.execute("Flink Stream Java API Skeleton");
    }

    public static void backPressureLow() throws Exception {
        streamEnv
                .fromCollection(IntStream.range(0, 1000000).boxed().collect(Collectors.toList()),
                        Types.INT)
                .flatMap((Integer tuple, Collector<Integer> out) -> {
                    IntStream.range(0, 300).boxed().forEach(num -> out.collect(tuple));
                })
                .returns(Types.INT)
                .setParallelism(2)
                .filter(tuple -> new Random().nextInt(70) == 0)
                .setParallelism(3)
                .print()
                .setParallelism(1);
        streamEnv.execute("Flink Stream Java API Skeleton");
    }

    public static void backPressureNo() throws Exception {
        streamEnv
                .fromCollection(IntStream.range(0, 1000000).boxed().collect(Collectors.toList()),
                        Types.INT)
                .flatMap((Integer tuple, Collector<Integer> out) -> {
                    IntStream.range(0, 300).boxed().forEach(num -> out.collect(tuple));
                })
                .returns(Types.INT)
                .setParallelism(2)
                .filter(tuple -> new Random().nextInt(700) == 0)
                .setParallelism(10)
                .print()
                .setParallelism(1);
        streamEnv.execute("Flink Stream Java API Skeleton");
    }
}
