package com.flink.demo.java.homework;

import com.flink.demo.java.netcat.NetCatConsole;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;


public class Lecture01 {

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
