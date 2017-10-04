package com.tomekl007.jobs;

import com.tomekl007.jobs.com.tomekl007.utils.ElementsInWindowCounter;
import com.tomekl007.jobs.com.tomekl007.utils.ElementsInWindowCounterKeyed;
import flink.stream.contrib.DataStreamUtils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TimeWindowTest {
    @Test
    public void givenStreamOfEvents_whenProcessEvents_thenShouldApplyTumblingWindowingOnTransformation() throws Exception {
        //given
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Tuple2<Integer, ZonedDateTime>> windowed = env.fromElements(
                new Tuple2<>(15, ZonedDateTime.now()
                        .withHour(1)
                        .withMinute(2)
                        .withSecond(2)
                        .truncatedTo(ChronoUnit.SECONDS)),
                new Tuple2<>(16,
                        ZonedDateTime.now()
                                .withHour(1)
                                .withMinute(2)
                                .withSecond(3)
                                .truncatedTo(ChronoUnit.SECONDS)),
                new Tuple2<>(17, ZonedDateTime.now()
                        .withHour(1)
                        .withMinute(2)
                        .withSecond(8)
                        .truncatedTo(ChronoUnit.SECONDS)),
                new Tuple2<>(18, ZonedDateTime.now()
                        .withHour(1)
                        .withMinute(2)
                        .withSecond(15)
                        .truncatedTo(ChronoUnit.SECONDS))
        ).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Integer, ZonedDateTime>>() {
            @Override
            public long extractAscendingTimestamp(Tuple2<Integer, ZonedDateTime> element) {
                return element.f1.toInstant().toEpochMilli();
            }
        });

        SingleOutputStreamOperator<Long> reduced = windowed
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(4)))
                .apply(new ElementsInWindowCounter());

        //when
        List<Long> windows = DataStreamUtils.collect(reduced);

        //then
        assertThat(windows.size()).isBetween(2, 3);
    }

    @Test
    public void givenStreamOfEvents_whenProcessEvents_thenShouldApplySlidingWindowingOnTransformation() throws Exception {
        //given
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Tuple2<Integer, ZonedDateTime>> windowed = env.fromElements(
                new Tuple2<>(15, ZonedDateTime.now().plusSeconds(2)),
                new Tuple2<>(16, ZonedDateTime.now().plusSeconds(3)),
                new Tuple2<>(17, ZonedDateTime.now().plusSeconds(8)),
                new Tuple2<>(18, ZonedDateTime.now().plusMinutes(25))
        ).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Integer, ZonedDateTime>>() {
            @Override
            public long extractAscendingTimestamp(Tuple2<Integer, ZonedDateTime> element) {
                return element.f1.toInstant().toEpochMilli();
            }
        });

        SingleOutputStreamOperator<Long> reduced = windowed
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                .apply(new ElementsInWindowCounter());

        //when
        List<Long> windows = DataStreamUtils.collect(reduced);

        //then
        assertThat(windows.size()).isGreaterThanOrEqualTo(5);
    }


    @Test
    public void givenStreamOfEvents_whenProcessEvents_thenShouldApplySessionWindowingOnTransformation() throws Exception {
        //given
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Tuple2<Integer, ZonedDateTime>> windowed = env.fromElements(
                new Tuple2<>(1, ZonedDateTime.now().plusSeconds(11)),
                new Tuple2<>(2, ZonedDateTime.now().plusSeconds(12)),
                new Tuple2<>(3, ZonedDateTime.now().plusSeconds(20)),
                new Tuple2<>(4, ZonedDateTime.now().plusMinutes(21)),
                new Tuple2<>(4, ZonedDateTime.now().plusMinutes(30))

        ).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Integer, ZonedDateTime>>() {
            @Override
            public long extractAscendingTimestamp(Tuple2<Integer, ZonedDateTime> element) {
                return element.f1.toInstant().toEpochMilli();
            }
        });

        SingleOutputStreamOperator<Long> reduced = windowed
                .windowAll(EventTimeSessionWindows.withGap(Time.seconds(7)))
                .apply(new ElementsInWindowCounter());

        //when
        List<Long> windows = DataStreamUtils.collect(reduced);

        //then
        assertThat(windows.size()).isEqualTo(3);
    }

    @Test
    public void givenStreamOfEvents_whenProcessEventsKeyedPerUserId_thenShouldApplySessionWindowingOnTransformation() throws Exception {
        //given
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Tuple2<Integer, ZonedDateTime>> windowed = env.fromElements(
                new Tuple2<>(1, ZonedDateTime.now().plusSeconds(11)),//start first session
                new Tuple2<>(1, ZonedDateTime.now().plusSeconds(12)),
                new Tuple2<>(2, ZonedDateTime.now().plusSeconds(12)),//start first session
                new Tuple2<>(1, ZonedDateTime.now().plusSeconds(20)),//start second
                new Tuple2<>(2, ZonedDateTime.now().plusSeconds(21))//start second

        ).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Integer, ZonedDateTime>>() {
            @Override
            public long extractAscendingTimestamp(Tuple2<Integer, ZonedDateTime> element) {
                return element.f1.toInstant().toEpochMilli();
            }
        });

        SingleOutputStreamOperator<Long> reduced = windowed
                .keyBy(new KeySelector<Tuple2<Integer, ZonedDateTime>, Integer>() {
                    @Override
                    public Integer getKey(Tuple2<Integer, ZonedDateTime> t) throws Exception {
                        return t.f0;
                    }
                })
                .window(EventTimeSessionWindows.withGap(Time.seconds(7)))
                .apply(new ElementsInWindowCounterKeyed());

        //when
        List<Long> windows = DataStreamUtils.collect(reduced);

        //then
        assertThat(windows.size()).isEqualTo(2);
    }

}
