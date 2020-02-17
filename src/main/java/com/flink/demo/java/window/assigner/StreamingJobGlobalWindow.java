package com.flink.demo.java.window.assigner;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class StreamingJobGlobalWindow {

    public static void main(String[] args) throws Exception {

        // http://localhost:9191/
        // http://localhost:9191/#/overview
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        streamEnv.setParallelism(1).setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        streamEnv
                .fromElements(
                        new Order("2008-08-08 08:08:08", 100L, 10001L, 10L, 1000L),
                        new Order("2008-08-08 08:08:09", 101L, 10002L, 15L, 2000L),
                        new Order("2008-08-08 08:08:10", 100L, 10003L, 20L, 3000L),
                        new Order("2008-08-08 08:08:21", 101L, 10004L, 25L, 4000L),
                        new Order("2008-08-08 08:08:22", 100L, 10005L, 30L, 5000L),
                        new Order("2008-08-08 08:08:23", 101L, 10006L, 35L, 6000L)
                )
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(Order element) {
                        return element.timestamp;
                    }
                })
                .keyBy("userId")
                .window(GlobalWindows.create());

    }

    @Data
    @NoArgsConstructor
    public static class Order {
        private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        private long timestamp;
        private long userId;
        private long itemId;
        private long amount;
        private long price;
        public Order (String timestamp, long userId, long itemId, long amount, long price) {
            this.timestamp = LocalDateTime.parse(timestamp, timeFormatter).toEpochSecond(ZoneOffset.UTC) * 1000;
            this.userId = userId;
            this.itemId = itemId;
            this.amount = amount;
            this.price = price;
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
