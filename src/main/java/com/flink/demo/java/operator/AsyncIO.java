package com.flink.demo.java.operator;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.*;

public class AsyncIO {

    public static void main(String[] args) throws Exception {
        // http://localhost:9191/
        // http://localhost:9191/#/overview
        // Overview -> Running Jobs -> Overview -> Accumulator
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        streamEnv.setParallelism(4).setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        DataStream<String> dataStream = streamEnv.fromElements("a", "b", "c", "d");
        DataStream<Tuple2<String, Integer>> resultStream =
                AsyncDataStream.orderedWait(dataStream, new AsyncIORequest(), 5000, TimeUnit.MILLISECONDS, 100);
        resultStream.print();
        streamEnv.execute("Flink Stream Java API Skeleton");
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class WordCount {
        private String word;
        private int count;
    }

    static class AsyncIORequest extends RichAsyncFunction<String, Tuple2<String, Integer>> {

        /**
         * The database specific client that can issue concurrent requests with callbacks.
         */
        // private transient DatabaseClient client;
        private static ExecutorService pool = Executors.newCachedThreadPool();

        @Override
        public void open(Configuration parameters) throws Exception {
            // client = new DatabaseClient(host, post, credentials);
        }

        @Override
        public void close() throws Exception {
            // client.close();
        }

        @Override
        public void timeout(String input, ResultFuture<Tuple2<String, Integer>> resultFuture) throws Exception {
            resultFuture.completeExceptionally(
                    new TimeoutException("Async function call has time out.")
            );
        }

        @Override
        public void asyncInvoke(String key, ResultFuture<Tuple2<String, Integer>> resultFuture) throws Exception {

            // issue the asynchronous request, receive a future for result
            // final Future<String> result = client.query(key);
            Future<Integer> result = pool.submit(() -> {
                // Async I/O operation
                return 3;
            });
            CompletableFuture.supplyAsync(() -> {
                try {
                    return result.get();
                } catch (InterruptedException | ExecutionException ex) {
                    System.out.println("Failed to get result of Async IO : " + ex);
                    return null;
                }
            }).thenAccept((Integer ioResult) -> {
                resultFuture.complete(Collections.singleton(new Tuple2<>(key, ioResult)));
            });
        }
    }
}
