package com.flink.demo.java.window.function;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class StreamingJobReduceFunction {

    public static void main(String[] args) throws Exception {

        // http://localhost:9191/
        // http://localhost:9191/#/overview
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        streamEnv.setParallelism(1).setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        streamEnv.getConfig().setAutoWatermarkInterval(1);
        streamEnv
                .fromElements(
                        new Order("2008-08-08 08:08:06", 100L, 10L, 1000L),
                        new Order("2008-08-08 08:08:07", 101L, 15L, 2000L),
                        new Order("2008-08-08 08:08:08", 100L, 10L, 1000L),
                        new Order("2008-08-08 08:08:09", 101L, 15L, 2000L),
                        new Order("2008-08-08 08:08:10", 100L, 20L, 3000L),
                        new Order("2008-08-08 08:08:11", 101L, 25L, 4000L),
                        new Order("2008-08-08 08:08:12", 100L, 30L, 5000L),
                        new Order("2008-08-08 08:08:13", 101L, 35L, 6000L)
                )
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(Order element) {
                        return element.getTs();
                    }
                })
                .keyBy("userId")
                .timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<Order>() {
                    @Override
                    public Order reduce(Order order1, Order order2) throws Exception {
                        return new Order(order1.timestamp,
                                order1.userId,
                                order1.amount + order2.amount,
                                order1.total + order2.total);
                    }
                }).print();
        streamEnv.execute("Flink Stream Java API Skeleton");

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        private String timestamp;
        private long userId;
        private long amount;
        private long total;
        public long getTs() {
            return LocalDateTime.parse(timestamp, timeFormatter).toEpochSecond(ZoneOffset.UTC) * 1000;
        }
    }

    @Data
    @NoArgsConstructor
    public static class OrderSummary {
        private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        private String windowStart;
        private String windowEnd;
        private long userId;
        private long orderNum;
        private long amountNum;
        private long total;
        public OrderSummary(long start, long end, long userId, long orderNum, long amountNum, long total) {
            this.windowStart = Instant.ofEpochMilli(start).atZone(ZoneId.systemDefault()).format(timeFormatter);
            this.windowEnd = Instant.ofEpochMilli(end).atZone(ZoneId.systemDefault()).format(timeFormatter);
            this.userId = userId;
            this.orderNum = orderNum;
            this.amountNum = amountNum;
            this.total = total;
        }
    }

}
