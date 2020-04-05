package com.example.java.naive.csv;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.concurrent.TimeUnit;

public class BatchJobCsvReader {
    public static void main(String[] args) {

        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
        }};
        final ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

//        DataSet<RecordDto> csvInput = batchEnv.readCsvFile(csvFilePath)
//                .pojoType(RecordDto.class, "playerName", "country", "year", "game", "gold", "silver", "bronze", "total");
//
//        csvInput.map(new MapFunction<RecordDto, RecordDto>() {
//            @Override
//            public RecordDto map(RecordDto value) throws Exception {
//                LOGGER.info("execute map:{}",value);
//                TimeUnit.SECONDS.sleep(5);
//                return value;
//            }
//        }).print();
    }
}
