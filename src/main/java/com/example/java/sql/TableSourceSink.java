package com.example.java.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

public class TableSourceSink {

    private static ExecutionEnvironment batchEnv;
    private static TableEnvironment batchTableEnv;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration() {{
            setInteger("rest.port", 9191);
            setBoolean("local.start-webserver", true);
            setString("log.file", "file:///tmp/test");
        }};
        batchEnv = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        batchTableEnv = BatchTableEnvironment.create(batchEnv);

        selectorForTuple();
    }

    public static void selectorForTuple() throws Exception {

        TableSource studentTableSource = CsvTableSource.builder()
                .path("src/main/resources/student.csv")
                .lineDelimiter("\\n+")
                .fieldDelimiter(",")
                .field("id", Types.INT)
                .field("name", Types.STRING)
                .field("age", Types.INT)
                .build();
        String[] fieldNames = {"id", "name", "age"};
        TypeInformation[] fieldTypes = {Types.INT, Types.STRING, Types.INT};
        TableSink studentTableSink = new CsvTableSink("result/result_student.csv", ",");
        batchTableEnv.registerTableSource("student", studentTableSource);
        batchTableEnv.registerTableSink("result_student", fieldNames, fieldTypes, studentTableSink);
        batchTableEnv.sqlUpdate("insert into result_student select id, name, age + 10 from student");

        batchEnv.execute();
        // print() will call execute()
        // env.execute("Flink Batch Java API Skeleton");
    }

}
