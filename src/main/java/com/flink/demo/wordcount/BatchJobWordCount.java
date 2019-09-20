package com.flink.demo.wordcount;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type;
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
public class BatchJobWordCount {

    public static void main(String[] args) throws Exception {
        countWord();
        countWordSkipStopWords();
    }

    public static void countWord() throws Exception {
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        DataSet<String> text = batchEnv.fromElements(
                "apache flink",
                "flink stream",
                "flink batch",
                "flink sql",
                "flink graph"
        );
        text.flatMap((String line, Collector<Tuple2<String, Integer>> out) ->
                Stream.of(line.split("¥¥s+"))
                        .forEach(value -> out.collect(Tuple2.of(value, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0)
                .sum(1)
                .print();

        // print() will call execute()
        // env.execute("Flink Batch Java API Skeleton");
    }

    public static void countWordSkipStopWords() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.fromElements(
                " Who is there ? ",
                " I think I hear them. Stand, ho ! Who is there ? ");
        DataSet<Tuple2<String, Integer>> wordCounts = text
                .flatMap(new LineSplitter())
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0)
                .sum(1);
        wordCounts.print();
    }

    /**
     * Skip Stop Words
     */
    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.split(" ")) {
                if (! ("".equals(word) || "?".equals(word) || "!".equals(word))) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        }
    }
}
