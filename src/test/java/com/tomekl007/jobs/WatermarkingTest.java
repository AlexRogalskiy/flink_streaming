package com.tomekl007.jobs;


import com.tomekl007.jobs.abandonedcart.CartEvent;
import flink.stream.contrib.DataStreamUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Ignore;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


public class WatermarkingTest {

    @Ignore("still problem")
    @Test
    public void givenStreamOfEvents_whenSumWindowsWithoutAllowingLateness_thenShouldSumOnlyInOrderEvents() throws Exception {
        //given
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple2<Integer, ZonedDateTime>> windowed = env.fromElements(
                new Tuple2<>(1, ZonedDateTime.now()
                        .withHour(1)
                        .withMinute(2)
                        .withSecond(2)
                        .truncatedTo(ChronoUnit.SECONDS)),
                new Tuple2<>(2, ZonedDateTime.now()
                        .withHour(1)
                        .withMinute(2)
                        .withSecond(3)
                        .truncatedTo(ChronoUnit.SECONDS)),
                new Tuple2<>(3, ZonedDateTime.now()
                        .withHour(1)
                        .withMinute(2)
                        .withSecond(8)
                        .truncatedTo(ChronoUnit.SECONDS)),
                new Tuple2<>(4, ZonedDateTime.now()
                        .withHour(1)
                        .withMinute(1)
                        .withSecond(1)
                        .truncatedTo(ChronoUnit.SECONDS))
        )
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple2<Integer, ZonedDateTime>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple2<Integer, ZonedDateTime> element) {
                                return element.f1.toInstant().toEpochMilli();
                            }
                        });

        SingleOutputStreamOperator<Integer> reduced = windowed
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<Integer, ZonedDateTime>>() {
                    @Override
                    public Tuple2<Integer, ZonedDateTime> reduce(Tuple2<Integer, ZonedDateTime> t1,
                                                                 Tuple2<Integer, ZonedDateTime> t2) throws Exception {
                        System.out.println("t1: " + t1 + " t2: " + t2);
                        return Tuple2.of(t1.f0 + t2.f0, ZonedDateTime.now());
                    }
                })
                .map(new MapFunction<Tuple2<Integer, ZonedDateTime>, Integer>() {
                    @Override
                    public Integer map(Tuple2<Integer, ZonedDateTime> t) throws Exception {
                        System.out.println("t: " + t);
                        return t.f0;
                    }
                });

        //when
        List<Integer> windows = DataStreamUtils.collect(reduced);
        System.out.println(windows);
        //then
        assertThat(windows.size()).isEqualTo(2);
        assertThat(windows.get(0)).isEqualTo(3);
        assertThat(windows.get(1)).isEqualTo(3);
    }

    @Test
    @Ignore
    public void givenStreamOfEvents_whenSumWindowsWithAllowingLateness_thenShouldSumAlsoNotInOrderEvent() throws Exception {
        //given
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple2<Integer, ZonedDateTime>> windowed = env.fromElements(
                new Tuple2<>(1, ZonedDateTime.now()
                        .withHour(1)
                        .withMinute(2)
                        .withSecond(2)
                        .truncatedTo(ChronoUnit.SECONDS)),
                new Tuple2<>(2, ZonedDateTime.now()
                        .withHour(1)
                        .withMinute(2)
                        .withSecond(3)
                        .truncatedTo(ChronoUnit.SECONDS)),
                new Tuple2<>(3, ZonedDateTime.now()
                        .withHour(1)
                        .withMinute(2)
                        .withSecond(8)
                        .truncatedTo(ChronoUnit.SECONDS)),
                new Tuple2<>(4, ZonedDateTime.now()
                        .withHour(1)
                        .withMinute(1)
                        .withSecond(50)
                        .truncatedTo(ChronoUnit.SECONDS))
        )
                .assignTimestampsAndWatermarks(

                        new BoundedOutOfOrdernessTimestampExtractor<Tuple2<Integer, ZonedDateTime>>(Time.seconds(20)) {
                            @Override
                            public long extractTimestamp(Tuple2<Integer, ZonedDateTime> element) {
                                return element.f1.toInstant().toEpochMilli();
                            }
                        });

        SingleOutputStreamOperator<Integer> reduced = windowed
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                .reduce(new ReduceFunction<Tuple2<Integer, ZonedDateTime>>() {
                    @Override
                    public Tuple2<Integer, ZonedDateTime> reduce(Tuple2<Integer, ZonedDateTime> t1,
                                                                 Tuple2<Integer, ZonedDateTime> t2) throws Exception {
                        System.out.println("t1: " + t1 + " t2: " + t2);
                        return Tuple2.of(t1.f0 + t2.f0, t1.f1);
                    }
                })
                .map(new MapFunction<Tuple2<Integer, ZonedDateTime>, Integer>() {
                    @Override
                    public Integer map(Tuple2<Integer, ZonedDateTime> t) throws Exception {
                        System.out.println("t: " + t);
                        return t.f0;
                    }
                });

        //when
        List<Integer> windows = DataStreamUtils.collect(reduced);
        System.out.println(windows);
        //then
        assertThat(windows.size()).isEqualTo(3);
    }
}
