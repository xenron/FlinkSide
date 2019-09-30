package com.flink.demo.java.join;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class TumblingWindowJoin {
    public static void main(String[] args) {
        // 10 second
        int windowSize = 10;
        // 5.1 second
        // long delay = 5100L;
        long delay = 10000L;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<Tuple3<String, String, Long>> leftSource = env.addSource(new StreamDataSouce1()).name("Demo Source");
        DataStream<Tuple3<String, String, Long>> rightSource = env.addSource(new StreamDataSouce2()).name("Demo Source");

        DataStream<Tuple3<String, String, Long>> leftStream = leftSource.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.milliseconds(delay)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element) {
                        return element.f2;
                    }
                }
        );
        DataStream<Tuple3<String, String, Long>> rightStream = rightSource.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.milliseconds(delay)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element) {
                        return element.f2;
                    }
                }
        );

        leftSource.join(rightSource)
//        leftStream.join(rightStream)
                .where(new LeftSelectKey())
                .equalTo(new RightSelectKey())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .apply(new JoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple5<String, String, String, Long, Long>>() {

                    @Override
                    public Tuple5<String, String, String, Long, Long> join(Tuple3<String, String, Long> leftItem, Tuple3<String, String, Long> rightItem) throws Exception {
                        return new Tuple5<>(leftItem.f0, leftItem.f1, rightItem.f1, leftItem.f2, rightItem.f2);
                    }
                })
                .print();

        leftStream.coGroup(rightStream)
                .where(new LeftSelectKey())
                .equalTo(new RightSelectKey())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .apply(new LeftJoin())
                .print();

    }

    public static class LeftSelectKey implements KeySelector<Tuple3<String, String, Long>, String> {

        @Override
        public String getKey(Tuple3<String, String, Long> element) throws Exception {
            return element.f0;
        }

    }

    public static class RightSelectKey implements KeySelector<Tuple3<String, String, Long>, String> {

        @Override
        public String getKey(Tuple3<String, String, Long> element) throws Exception {
            return element.f0;
        }
    }

    public static class LeftJoin implements CoGroupFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple5<String, String, String, Long, Long>> {

        @Override
        public void coGroup(Iterable<Tuple3<String, String, Long>> leftElements, Iterable<Tuple3<String, String, Long>> rightElements, Collector<Tuple5<String, String, String, Long, Long>> out) throws Exception {
            for (Tuple3<String, String, Long> leftElement : leftElements) {
                boolean hadElements = false;
                for (Tuple3<String, String, Long> rightElement : rightElements) {
                    out.collect(new Tuple5<>(leftElement.f0, leftElement.f1, rightElement.f1, leftElement.f2, rightElement.f2));
                    hadElements = true;
                }
                if (!hadElements) {
                    out.collect(new Tuple5<>(leftElement.f0, leftElement.f1, "null", leftElement.f2, -1L));
                }
            }
        }

    }

}
