package com.flink.demo.java.naive;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

public class BatchJobReturnType {

    public static void main(String[] args) throws Exception {
        String[] inputArray = {
                "apache flink",
                "flink stream",
                "flink batch",
                "flink sql",
                "flink graph"
        };
        selectorForTuple(inputArray);
        // ResultTypeQueryable
    }

    public static void selectorForTuple(String[] inputArray) throws Exception {
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataSet<String> text = batchEnv.fromElements(inputArray);
        text.flatMap((String line, Collector<Tuple2<String, Integer>> out) ->
                Stream.of(line.split("\\s+"))
                        .forEach(value -> out.collect(Tuple2.of(value, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0)
                .sum(1)
                .print();

        // print() will call execute()
        // env.execute("Flink Batch Java API Skeleton");
    }

}

