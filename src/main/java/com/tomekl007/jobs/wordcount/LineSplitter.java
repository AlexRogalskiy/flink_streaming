package com.tomekl007.jobs.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

@SuppressWarnings("serial")
public class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {

        String[] tokens = new String[]{};//todo implement split
        Stream.of(tokens)
                .filter(t -> t.length() > 0)
                .forEach(token ->
                        out.collect(Tuple2.of("", 0))//todo collect token with counter
                );
    }
}