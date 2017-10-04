package com.tomekl007.jobs;


import com.tomekl007.jobs.com.tomekl007.utils.ElementsInWindowCounter;
import flink.stream.contrib.DataStreamUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.List;


public class WatermarkingTest {
    @Test
    public void gaivenStreamOfEvents_whenProcessEvents_thenShouldApplyWindowingOnTransformation() throws Exception {
        //given
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Tuple2<Integer, ZonedDateTime>> windowed = env.fromElements(
                new Tuple2<>(15, ZonedDateTime.now().plusSeconds(2)),
                new Tuple2<>(16, ZonedDateTime.now().plusSeconds(3)),
                new Tuple2<>(17, ZonedDateTime.now().plusSeconds(8)),
                new Tuple2<>(18, ZonedDateTime.now().plusSeconds(15))
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<Integer, ZonedDateTime>>(Time.seconds(20)) {
            @Override
            public long extractTimestamp(Tuple2<Integer, ZonedDateTime> element) {
                return element.f1.toEpochSecond() * 1000;
            }
        });

        SingleOutputStreamOperator<Long> reduced = windowed
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new ElementsInWindowCounter());

        //when
        List<Long> result = DataStreamUtils.collect(reduced);

        //then
        System.out.println(result);
    }
}
