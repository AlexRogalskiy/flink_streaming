package com.tomekl007.jobs.com.tomekl007.utils;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.ZonedDateTime;


public class ElementsInWindowCounter implements AllWindowFunction<Tuple2<Integer, ZonedDateTime>, Long, TimeWindow> {
    @Override
    public void apply(TimeWindow window, Iterable<Tuple2<Integer, ZonedDateTime>> values, Collector<Long> out) throws Exception {
        long count = 0;
        System.out.println("values: " + Lists.newArrayList(values));
        for (Tuple2<Integer, ZonedDateTime> i : values) {
            count++;
        }
        out.collect(count);
    }
}