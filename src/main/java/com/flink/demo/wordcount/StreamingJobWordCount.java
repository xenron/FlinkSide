package com.flink.demo.wordcount;

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
 *       先在node21机器上执行nc -l 9000
 */
public class StreamingJobWordCount {
    public static void main(String[] args) throws Exception {
        countWord01();
    }

    public static void countWord01() throws Exception {
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        streamEnv.setParallelism(4).setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        streamEnv
                .socketTextStream("localhost",9999)
                .flatMap((String line, Collector<KeyCount> out) -> {
                    Stream.of(line.split("¥¥s+"))
                            .forEach(value -> out.collect(new KeyCount(value, 1)));
                })
                .returns(Types.POJO(KeyCount.class))
                .keyBy(new KeySelector<KeyCount, String>() {
                    @Override
                    public String getKey(KeyCount value) throws Exception {
                        return value.getKey();
                    }
                })
                .timeWindow(Time.seconds(10))
                .sum("count")
                .print();
        streamEnv.execute("Flink Stream Java API Skeleton");
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class KeyCount {
        private String key;
        private int count;
    }

    public static void countWord02(String[] args) throws Exception {
        // String hostname = "211.1xx.1xx.6x";
        String hostname = "192.168.101.184";
        // 定义socket的端口号
        int port = 9000;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("没有指定port参数，使用默认值6100");
            port = 6100;
        }
        // 获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 连接socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream(hostname, port,"\n");
        // 本文填写的是远程linux ip，在远程linux上需要执行：nc -l 6100命令
        // 计算数据
        DataStream<WordWithCount> windowCount = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                String[] splits = value.split("\\s");
                for (String word:splits) {
                    out.collect(new WordWithCount(word,1L));
                }
            }
        })// 打平操作，把每行的单词转为<word,count>类型的数据
                // 针对相同的word数据进行分组
                .keyBy("word")
                // 指定计算数据的窗口大小和滑动窗口大小
                .timeWindow(Time.seconds(5),Time.seconds(1))
                .sum("count");
        // 把数据打印到控制台,使用一个并行度
        windowCount.print().setParallelism(1);
        // 注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("streaming word count");
    }

    /**
     * 主要为了存储单词以及单词出现的次数
     */
    public static class WordWithCount{
        public String word;
        public long count;
        public WordWithCount(){}
        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

}

