package com.flink.demo.java.join;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class StreamDataSouce1 extends RichParallelSourceFunction<Tuple3<String, String, Long>> {
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> sourceContext) throws Exception {
        Tuple3[] elements = new Tuple3[]{
                // 2001-09-09 09:47:30
                Tuple3.of("a", "1", 1000000050000L),
                // 2001-09-09 09:47:34
                Tuple3.of("a", "2", 1000000054000L),
                // 2001-09-09 09:47:59
                Tuple3.of("a", "3", 1000000079900L),
                // 2001-09-09 09:48:35
                Tuple3.of("a", "4", 1000000115000L),
                // 2001-09-09 09:48:20
                Tuple3.of("b", "5", 1000000100000L),
                // 2001-09-09 09:48:28
                Tuple3.of("b", "6", 1000000108000L),
        };
        int count = 0;
        while (running && count < elements.length) {
            sourceContext.collect(new Tuple3<>((String) elements[count].f0,
                    (String) elements[count].f1,
                    (Long) elements[count].f2));
            count++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
