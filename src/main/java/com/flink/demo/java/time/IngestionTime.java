package com.flink.demo.java.time;

import lombok.AllArgsConstructor;
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
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static java.lang.Thread.sleep;

public class IngestionTime {

    public static void main(String[] args) throws Exception {

        // http://localhost:9191/
        // http://localhost:9191/#/overview
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        streamEnv.setParallelism(1).setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        streamEnv
                .fromElements(
                        // 2008-08-08 16:08:08
                        new Order(1218182888000L, 100L, 10001L, 10L, 1000L),
                        new Order(1218182889000L, 101L, 10002L, 15L, 2000L),
                        new Order(1218182890000L, 100L, 10003L, 20L, 3000L),
                        new Order(1218182891000L, 101L, 10004L, 25L, 4000L),
                        new Order(1218182892000L, 100L, 10005L, 30L, 5000L),
                        new Order(1218182893000L, 101L, 10006L, 35L, 6000L)
                )
                .keyBy("userId")
                .timeWindow(Time.seconds(5))
                .aggregate(new AggregateFunction<Order, Tuple4<Long, Long, Long, Long>, Tuple4<Long, Long, Long, Long>>() {
                    @Override
                    public Tuple4<Long, Long, Long, Long> createAccumulator() {
                        return Tuple4.of(0L, 0L, 0L, 0L);
                    }

                    @Override
                    public Tuple4<Long, Long, Long, Long> add(Order value, Tuple4<Long, Long, Long, Long> accumulator) {
                        try {
                            sleep(3000L);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        return Tuple4.of(value.userId,
                                accumulator.f1 + 1,
                                accumulator.f2 + value.amount,
                                accumulator.f3 + value.amount * value.price);
                    }

                    @Override
                    public Tuple4<Long, Long, Long, Long> getResult(Tuple4<Long, Long, Long, Long> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Tuple4<Long, Long, Long, Long> merge(Tuple4<Long, Long, Long, Long> a, Tuple4<Long, Long, Long, Long> b) {
                        return Tuple4.of(a.f0, a.f1 + b.f1, a.f2 + b.f2, a.f3 + b.f3);
                    }
                }, new WindowFunction<Tuple4<Long, Long, Long, Long>, OrderSummary, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple4<Long, Long, Long, Long>> input, Collector<OrderSummary> out) throws Exception {
                        input.forEach(record -> out.collect(new OrderSummary(window.getStart(), window.getEnd(), record.f0, record.f1, record.f2, record.f3)));
                    }
                }).print();
        streamEnv.execute("Flink Stream Java API Skeleton");

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        private long timestamp;
        private long userId;
        private long itemId;
        private long amount;
        private long price;
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
