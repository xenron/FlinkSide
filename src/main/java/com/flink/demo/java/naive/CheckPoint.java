package com.flink.demo.java.naive;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CheckPoint {
    static StreamExecutionEnvironment streamEnv;
    public static void main(String[] args) throws Exception {
        // http://localhost:9191/
        // http://localhost:9191/#/overview
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        streamEnv.enableCheckpointing(2000, CheckpointingMode.AT_LEAST_ONCE);
        streamEnv.setStateBackend(new MemoryStateBackend());

        withHeavyBackPressure();
        withoutHeavyBackPressure();
    }

    public static void withHeavyBackPressure() throws Exception {
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


    public static void withoutHeavyBackPressure() throws Exception {
        streamEnv.addSource(new SourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                IntStream.range(0, 1000000).boxed()
                        .forEach(num -> {
                            try {
                                Thread.sleep(5);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            sourceContext.collect(num);
                        });
            }

            @Override
            public void cancel() {

            }
        })
                .returns(Types.INT)
                .flatMap((Integer tuple, Collector<Tuple2<Integer, Integer>> out) -> {
                    IntStream.range(0, 300).boxed()
                            .forEach(num -> out.collect(Tuple2.of(num, tuple)));
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
