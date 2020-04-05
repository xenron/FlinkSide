package com.example.java.naive.key;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

/**
 * Author: Mr.Deng
 * Date: 2018/10/15
 * Desc: 使用flink对指定窗口内的数据进行实时统计，最终把结果打印出来
 * 先在node21机器上执行nc -l 9000
 */
public class StreamingJobKeySelect {
    public static void main(String[] args) throws Exception {
        countWord01();
    }

    public static void countWord01() throws Exception {
        // http://localhost:9191/
        // http://localhost:9191/#/overview
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        streamEnv.setParallelism(2).setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        streamEnv
                .socketTextStream("localhost", 9999)
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    Stream.of(line.split("\\s+"))
                            .forEach(value -> out.collect(Tuple2.of(value, 1)));
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1)
                .print();
        streamEnv.execute("Flink Stream Java API Skeleton");
    }

}

