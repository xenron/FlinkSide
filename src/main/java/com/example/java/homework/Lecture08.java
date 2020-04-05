package com.example.java.homework;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;


public class Lecture08 {

    public static void main(String[] args) throws Exception {
        //定义socket的端口号
        int port = 9999;
        //获取运行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度为1,默认并行度是当前机器的cpu数量
        streamEnv.setParallelism(1);

        streamEnv.readFileStream("src/main/resources/csv", 1000, FileMonitoringFunction.WatchType.ONLY_NEW_FILES)
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    Stream.of(line.split("\\s+"))
                            .forEach(value -> out.collect(Tuple2.of(value, 1)));
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .timeWindow(Time.seconds(10))
                .sum(1)
                .print();

        streamEnv.execute("lecture 01 homework");

    }

}
