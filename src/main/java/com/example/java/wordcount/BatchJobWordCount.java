package com.example.java.wordcount;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

/**
 * Author: xenron
 * Desc:
 */
@Slf4j
public class BatchJobWordCount {

    public static void main(String[] args) throws Exception {
        countWord();
        countWordSkipStopWords();
    }

    public static void countWord() throws Exception {
        System.out.println("countWord start.");
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
                Stream.of(line.split("\\s+"))
                        .forEach(value -> out.collect(Tuple2.of(value, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0)
                .sum(1)
                .print();

        // print() will call execute()
        // env.execute("Flink Batch Java API Skeleton");
        System.out.println("countWord end.");
    }

    public static void countWordSkipStopWords() throws Exception {
        System.out.println("countWordSkipStopWords start.");

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String[] inputArray = {
                " Who is there ? ",
                " I think I hear them. Stand, ho ! Who is there ? "
        };
        DataSet<String> text = env.fromElements(inputArray);
        DataSet<Tuple2<String, Integer>> wordCounts = text
                .flatMap(new LineSplitter())
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0)
                .sum(1);
        wordCounts.print();

        // print() will call execute()
        // env.execute("Flink Batch Java API Skeleton");
        System.out.println("countWordSkipStopWords end.");
    }

    /**
     * skip stop words
     */
    private static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.split(" ")) {
                if (!("".equals(word) || "?".equals(word) || "!".equals(word))) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        }
    }
}
