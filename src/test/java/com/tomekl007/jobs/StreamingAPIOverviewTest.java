package com.tomekl007.jobs;

import flink.stream.contrib.DataStreamUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class StreamingAPIOverviewTest {

    @Test
    public void givenStreamOfEvents_whenProcessEvents_thenShouldPrintResultsOnSinkOperation() throws Exception {
        //given
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text
                = env.fromElements("This is a first sentence",
                "This is a second sentence with a one word");
        SingleOutputStreamOperator<String> upperCase = text.map(String::toUpperCase);


        //when
        List<String> strings = DataStreamUtils.collect(upperCase);

        //then
        assertThat(strings).contains("THIS IS A SECOND SENTENCE WITH A ONE WORD",
                "THIS IS A FIRST SENTENCE");
    }

    @Test
    @Ignore("infinite stream")
    public void givenIterativeDataStream_whenApplyOperations_thenShouldYieldProperResults() throws IOException {
        //given
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> someIntegers = env.generateSequence(0, 1000);

        IterativeStream<Long> iteration = someIntegers.iterate();

        //when
        DataStream<Long> minusOne = iteration
                .map((MapFunction<Long, Long>) value -> value - 1);

        DataStream<Long> stillGreaterThanZero = minusOne
                .filter((FilterFunction<Long>) value -> (value > 0));

        iteration.closeWith(stillGreaterThanZero);

        //then
        stillGreaterThanZero.print();
    }

}
