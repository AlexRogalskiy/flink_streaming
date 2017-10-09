package com.tomekl007.jobs;

import com.google.common.collect.Lists;
import flink.stream.contrib.DataStreamUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class SortingTest {
    @Test
    public void givenUnsortedStream_whenSortingIt_thenShouldHaveProperOrder() throws IOException {
        //given
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple2<Integer, ZonedDateTime>> windowed = env.fromElements(
                new Tuple2<>(3, ZonedDateTime.now()
                        .withHour(1)
                        .withMinute(2)
                        .withSecond(10)
                        .truncatedTo(ChronoUnit.SECONDS)),
                new Tuple2<>(2, ZonedDateTime.now()
                        .withHour(1)
                        .withMinute(2)
                        .withSecond(5)
                        .truncatedTo(ChronoUnit.SECONDS)),
                new Tuple2<>(1, ZonedDateTime.now()
                        .withHour(1)
                        .withMinute(2)
                        .withSecond(1)
                        .truncatedTo(ChronoUnit.SECONDS))
        ).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Integer, ZonedDateTime>>() {
            @Override
            public long extractAscendingTimestamp(Tuple2<Integer, ZonedDateTime> element) {
                return element.f1.toInstant().toEpochMilli();
            }
        });

        //when
        SingleOutputStreamOperator<Iterable<Tuple2<Integer, ZonedDateTime>>> sorted = windowed
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(15)))
                .allowedLateness(Time.seconds(20))
                .apply(new WindowSort());

        //then
        List<Iterable<Tuple2<Integer, ZonedDateTime>>> collect = DataStreamUtils.collect(sorted);
        List<Tuple2<Integer, ZonedDateTime>> firstWindow = Lists.newArrayList(collect.get(0));
        assertThat(firstWindow.get(0).f0).isEqualTo(1);
        assertThat(firstWindow.get(1).f0).isEqualTo(2);
        assertThat(firstWindow.get(2).f0).isEqualTo(3);


    }

    private class WindowSort implements AllWindowFunction<Tuple2<Integer, ZonedDateTime>, Iterable<Tuple2<Integer, ZonedDateTime>>, TimeWindow> {
        @Override
        public void apply(TimeWindow window,
                          Iterable<Tuple2<Integer, ZonedDateTime>> values,
                          Collector<Iterable<Tuple2<Integer, ZonedDateTime>>> out) throws Exception {
            System.out.println("values:" + values);
            List<Tuple2<Integer, ZonedDateTime>> sortedList = new ArrayList<>();
            for (Tuple2<Integer, ZonedDateTime> i : values) {
                sortedList.add(i);
            }
            sortedList.sort(new Comparator<Tuple2<Integer, ZonedDateTime>>() {
                @Override
                public int compare(Tuple2<Integer, ZonedDateTime> o1, Tuple2<Integer, ZonedDateTime> o2) {
                    return o1.f1.compareTo(o2.f1);
                }
            });
            out.collect(sortedList);
        }

    }
}
