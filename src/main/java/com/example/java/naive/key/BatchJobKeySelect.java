package com.example.java.naive.key;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

public class BatchJobKeySelect {

    public static void main(String[] args) throws Exception {
        String[] inputArray = {
                "apache flink",
                "flink stream",
                "flink batch",
                "flink sql",
                "flink graph"
        };
//        selectorForKeySelector();
        selectorForPojo(inputArray);
//        selectorForTupleOffset(inputArray);
//        selectorForTupleX();
    }

    public static void selectorForTupleOffset(String[] inputArray) throws Exception {
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        DataSet<String> text = batchEnv.fromElements(inputArray);
        text.flatMap((String line, Collector<Tuple2<String, Integer>> out) ->
                Stream.of(line.split(" "))
                        .forEach(value -> out.collect(Tuple2.of(value, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0)
                .sum(1)
                .print();

        // print() will call execute()
        // env.execute("Flink Batch Java API Skeleton");
    }

    public static void selectorForPojo(String[] inputArray) throws Exception {
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        DataSet<String> text = batchEnv.fromElements(inputArray);
        text.flatMap((String line, Collector<KeyCount> out) -> {
            Stream.of(line.split(" "))
                    .forEach(value -> out.collect(new KeyCount(value, 1)));
        })
                .returns(Types.POJO(KeyCount.class))
                .groupBy(
                        new KeySelector<KeyCount, String>() {
                            @Override
                            public String getKey(KeyCount value) throws Exception {
                                return value.getKey();
                            }
                        }
                )
                .reduce(new ReduceFunction<KeyCount>() {
                    @Override
                    public KeyCount reduce(KeyCount keyCount, KeyCount t1) throws Exception {
                        return new KeyCount(keyCount.key, keyCount.count + t1.count);
                    }
                })
                .print();

        // print() will call execute()
        // env.execute("Flink Batch Java API Skeleton");
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class KeyCount {
        private String key;
        private int count;
    }

}

