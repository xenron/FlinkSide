package com.flink.demo.utils;

import org.apache.flink.api.java.tuple.Tuple3;

import java.text.SimpleDateFormat;

public class DateUtils {
    public static String convertLongToTimestamp(long longTime) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return simpleDateFormat.format(longTime);
    }
    public static void main(String[] args) {
        System.out.println(convertLongToTimestamp(1218182888000L));
    }
}
