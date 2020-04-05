package com.example.java.operator.transformation;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import scala.Int;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class BatchJobTransformation {

    public static void main(String[] args) throws Exception {

        // only in batch
        // groupBy & sum
        groupByAndSum();

        // map & reduce
        mapAndReduce();
        groupByAndReduce();
        groupByAndReduceGroup();
        mapPartition();
        join();
        leftOutJoin();
        crossJoin();
        partitionByHash();
        partitionByRange();
        partitionByCustom();

        broadCast();
    }

    public static void groupByAndSum() throws Exception {
        System.out.println("groupByAndSum start.");
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        String[] inputArray = {
                "apache flink",
                "flink stream",
                "flink batch",
                "flink sql",
                "flink graph"
        };
        DataSet<String> text = batchEnv.fromElements(inputArray);
        text.flatMap((String line, Collector<Tuple2<String, Integer>> out) ->
                Stream.of(line.split(" "))
                        .forEach(value -> out.collect(Tuple2.of(value, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0)
                .sum(1)
                .print();

        System.out.println("groupByAndSum end.");
    }

    public static void mapAndReduce() throws Exception {
        System.out.println("mapAndReduce start.");
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        String[] inputArray = {
                "apache flink",
                "flink stream",
                "flink batch",
                "flink sql",
                "flink graph"
        };
        DataSet<String> text = batchEnv.fromElements(inputArray);
        text.flatMap((String line, Collector<Tuple2<String, Integer>> out) ->
                Stream.of(line.split(" "))
                        .forEach(value -> out.collect(Tuple2.of(value, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .map(tuple -> tuple.f1)
                .reduce((value1, value2) -> value1 + value2)
                .setCombineHint(ReduceOperatorBase.CombineHint.HASH)
                .print();

        System.out.println("mapAndReduce end.");
    }

    public static void groupByAndReduce() throws Exception {
        System.out.println("groupByAndReduce start.");
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        String[] inputArray = {
                "apache flink",
                "flink stream",
                "flink batch",
                "flink sql",
                "flink graph"
        };
        DataSet<String> text = batchEnv.fromElements(inputArray);
        text.flatMap((String line, Collector<Tuple2<String, Integer>> out) ->
                Stream.of(line.split(" "))
                        .forEach(value -> out.collect(Tuple2.of(value, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0)
                .reduce((value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1))
                .setCombineHint(ReduceOperatorBase.CombineHint.HASH)
                .print();

        System.out.println("groupByAndReduce end.");
    }

    public static void groupByAndReduceGroup() throws Exception {
        System.out.println("groupByAndReduceGroup start.");
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        String[] inputArray = {
                "apache flink",
                "flink stream",
                "flink batch",
                "flink sql",
                "flink graph"
        };
        DataSet<String> text = batchEnv.fromElements(inputArray);
        text.flatMap((String line, Collector<Tuple2<String, Integer>> out) ->
                Stream.of(line.split(" "))
                        .forEach(value -> out.collect(Tuple2.of(value, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0)
                .reduceGroup((Iterable<Tuple2<String, Integer>> tuples, Collector<Tuple3<String, Integer, Integer>> out) -> {
                    int count = 1;
                    for (Tuple2<String, Integer> tuple : tuples) {
                        out.collect(Tuple3.of(tuple.f0, tuple.f1, count));
                        count += 1;
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT))
                .print();

        System.out.println("groupByAndReduceGroup end.");
    }

    public static void mapPartition() throws Exception {
        System.out.println("mapPartition start.");
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        String[] inputArray = {
                "apache flink",
                "flink stream",
                "flink batch",
                "flink sql",
                "flink graph"
        };
        DataSet<String> text = batchEnv.fromElements(inputArray);
        text.flatMap((String line, Collector<Tuple2<String, Integer>> out) ->
                Stream.of(line.split(" "))
                        .forEach(value -> out.collect(Tuple2.of(value, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .mapPartition((Iterable<Tuple2<String, Integer>> tuples, Collector<Tuple2<String, Integer>> out) -> {
                    tuples.forEach(tuple -> out.collect(tuple));
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .print();

        System.out.println("mapPartition end.");
    }

    public static void join() throws Exception {
        System.out.println("join start.");
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        String[] wordArray = {
                "apache flink",
                "flink stream",
                "flink batch",
                "flink sql",
                "flink graph"
        };
        DataSet<Tuple2<String, Integer>> words = batchEnv.fromElements(wordArray)
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) ->
                        Stream.of(line.split(" "))
                                .forEach(value -> out.collect(Tuple2.of(value, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT));
        DataSet<Tuple2<String, String>> types = batchEnv.fromElements(
                Tuple2.of("apache", "world"),
                Tuple2.of("flink", "apache"),
                Tuple2.of("batch", "flink"),
                Tuple2.of("sql", "flink"),
                Tuple2.of("graph", "flink")
        );

        words.join(types, JoinOperatorBase.JoinHint.BROADCAST_HASH_FIRST)
                .where(0)
                .equalTo(0)
                .projectFirst(0, 1)
                .projectSecond(1)
                .print();

        System.out.println("join end.");
    }

    public static void leftOutJoin() throws Exception {
        System.out.println("leftOutJoin start.");
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        String[] wordArray = {
                "apache flink",
                "flink stream",
                "flink batch",
                "flink sql",
                "flink graph"
        };
        DataSet<Tuple2<String, Integer>> words = batchEnv.fromElements(wordArray)
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) ->
                        Stream.of(line.split(" "))
                                .forEach(value -> out.collect(Tuple2.of(value, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT));
        DataSet<Tuple2<String, String>> types = batchEnv.fromElements(
                Tuple2.of("apache", "world"),
                Tuple2.of("flink", "apache"),
                Tuple2.of("batch", "flink"),
                Tuple2.of("sql", "flink"),
                Tuple2.of("graph", "flink")
        );

        words.join(types, JoinOperatorBase.JoinHint.BROADCAST_HASH_FIRST)
                .where(0)
                .equalTo(0)
                .with((Tuple2<String, Integer> left, Tuple2<String, String> right) -> {
                    // Note:
                    // - v2 might be null for leftOutJoin
                    // - v1 might be null for rightOutJoin
                    // - v1 Or v2 might be null for fullOutJoin
                    return Tuple3.of(left.f0, left.f1, right.f1);
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.STRING))
                .print();

        System.out.println("leftOutJoin end.");
    }

    public static void crossJoin() throws Exception {
        System.out.println("crossJoin start.");
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        String[] wordArray = {
                "apache flink",
                "flink stream",
                "flink batch",
                "flink sql",
                "flink graph"
        };
        DataSet<Tuple2<String, Integer>> words = batchEnv.fromElements(wordArray)
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) ->
                        Stream.of(line.split(" "))
                                .forEach(value -> out.collect(Tuple2.of(value, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT));
        DataSet<Tuple2<String, String>> types = batchEnv.fromElements(
                Tuple2.of("apache", "world"),
                Tuple2.of("flink", "apache"),
                Tuple2.of("batch", "flink"),
                Tuple2.of("sql", "flink"),
                Tuple2.of("graph", "flink")
        );

        words.cross(types)
                .with((Tuple2<String, Integer> left, Tuple2<String, String> right) -> {
                    return Tuple3.of(left.f0, left.f1, right.f1);
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.STRING))
                .print();

        System.out.println("crossJoin end.");
    }

    public static void partitionByHash() throws Exception {
        System.out.println("partitionByHash start.");
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        batchEnv.setParallelism(3);
        DataSet<Tuple2<Integer, String>> dataset = batchEnv.fromElements(
                Tuple2.of(1, "a"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c"),
                Tuple2.of(4, "d"),
                Tuple2.of(5, "e"),
                Tuple2.of(6, "f"),
                Tuple2.of(7, "g"),
                Tuple2.of(8, "h")
        );
        dataset.partitionByHash(0)
                .map(tuple -> {
                    Thread.sleep(1000);
                    return Tuple3.of(tuple.f0, tuple.f1, Thread.currentThread().toString());
                })
                .returns(Types.TUPLE(Types.INT, Types.STRING, Types.STRING))
                .print();

        System.out.println("partitionByHash end.");
    }


    public static void partitionByRange() throws Exception {
        System.out.println("partitionByRange start.");
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        batchEnv.setParallelism(3);
        DataSet<Tuple2<Integer, String>> dataset = batchEnv.fromElements(
                Tuple2.of(1, "a"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c"),
                Tuple2.of(4, "d"),
                Tuple2.of(5, "e"),
                Tuple2.of(6, "f"),
                Tuple2.of(7, "g"),
                Tuple2.of(8, "h")
        );
        dataset.partitionByRange(0)
                .map(tuple -> {
                    Thread.sleep(1000);
                    return Tuple3.of(tuple.f0, tuple.f1, Thread.currentThread().toString());
                })
                .returns(Types.TUPLE(Types.INT, Types.STRING, Types.STRING))
                .print();

        System.out.println("partitionByRange end.");
    }


    public static void partitionByCustom() throws Exception {
        System.out.println("partitionByCustom start.");
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        batchEnv.setParallelism(3);
        DataSet<Tuple2<Integer, String>> dataset = batchEnv.fromElements(
                Tuple2.of(1, "a"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c"),
                Tuple2.of(4, "d"),
                Tuple2.of(5, "e"),
                Tuple2.of(6, "f"),
                Tuple2.of(7, "g"),
                Tuple2.of(8, "h")
        );
        dataset.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {
                return Math.abs(key) % numPartitions;
            }
        }, 0)
                .map(tuple -> {
                    Thread.sleep(1000);
                    return Tuple3.of(tuple.f0, tuple.f1, Thread.currentThread().toString());
                })
                .returns(Types.TUPLE(Types.INT, Types.STRING, Types.STRING))
                .print();

        System.out.println("partitionByCustom end.");
    }

    public static void broadCast() throws Exception {
        System.out.println("broadCast start.");
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        batchEnv.setParallelism(3);
        DataSet<Tuple2<String, Integer>> datasetBroadCast = batchEnv.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("b", 2),
                Tuple2.of("c", 3)
        );
        batchEnv.fromElements("a", "b", "c", "d")
                .map(new RichMapFunction<String, Integer>() {

                    private Map<String, Integer> wordCountMapping = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Collection<Tuple2<String, Integer>> broadCastMap = getRuntimeContext().getBroadcastVariable("word2count");
                        broadCastMap.forEach(tuple -> wordCountMapping.put(tuple.f0, tuple.f1));
                    }

                    @Override
                    public Integer map(String value) throws Exception {
                        return wordCountMapping.getOrDefault(value, 0);
                    }
                }).withBroadcastSet(datasetBroadCast, "word2count")
                .print();

        System.out.println("broadCast end.");
    }

    public static void distributedCache() throws Exception {
        System.out.println("distributedCache start.");
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        batchEnv.setParallelism(3);
        DataSet<Tuple2<Integer, String>> dataset = batchEnv.fromElements(
                Tuple2.of(1, "a"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c"),
                Tuple2.of(4, "d"),
                Tuple2.of(5, "e"),
                Tuple2.of(6, "f"),
                Tuple2.of(7, "g"),
                Tuple2.of(8, "h")
        );
        dataset.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {
                return Math.abs(key) % 3;
            }
        }, 0)
                .map(tuple -> {
                    Thread.sleep(1000);
                    return Tuple3.of(tuple.f0, tuple.f1, Thread.currentThread().toString());
                })
                .returns(Types.TUPLE(Types.INT, Types.STRING, Types.STRING))
                .print();

        System.out.println("distributedCache end.");
    }
}
