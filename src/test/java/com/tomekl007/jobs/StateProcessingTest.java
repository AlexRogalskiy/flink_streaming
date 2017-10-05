package com.tomekl007.jobs;

import com.tomekl007.jobs.state.CountWindowAverage;
import flink.stream.contrib.DataStreamUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class StateProcessingTest {
    @Test
    public void givenStreamOfInts_whenCalculateAverage_thenShouldReturnProperValue() throws IOException {
        //given
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //when
        SingleOutputStreamOperator<Tuple2<Long, Long>> average = env.fromElements(
                Tuple2.of(1L, 3L),
                Tuple2.of(1L, 5L),
                Tuple2.of(1L, 7L),
                Tuple2.of(1L, 5L),
                Tuple2.of(1L, 2L))
                .keyBy(0)
                .flatMap(new CountWindowAverage());
        //then
        List<Tuple2<Long, Long>> result = DataStreamUtils.collect(average);
        assertThat(result.size()).isEqualTo(2);
        assertThat(result.get(0).f1).isEqualTo(4L);
        assertThat(result.get(1).f1).isEqualTo(6L);
    }

    @Test
    public void givenStreamOfInts_whenCalculateAverage_thenShouldReturnProperValueWithCheckpointing() throws Exception {
        //given
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                2, // number of restart attempts
                Time.of(1, TimeUnit.SECONDS) // delay
        ));

        env.enableCheckpointing(10);
        env.setStateBackend(new MemoryStateBackend());

        //when
        SingleOutputStreamOperator<Tuple2<Long, Long>> average = env.fromElements(
                Tuple2.of(1L, 3L),
                Tuple2.of(1L, 5L),
                Tuple2.of(1L, 7L),
                Tuple2.of(1L, 5L),
                Tuple2.of(1L, 2L))
                .keyBy(0)
                .flatMap(new CountWindowAverage());

        //then
        List<Tuple2<Long, Long>> result = DataStreamUtils.collect(average);
        assertThat(result.size()).isEqualTo(2);
        assertThat(result.get(0).f1).isEqualTo(4L);
        assertThat(result.get(1).f1).isEqualTo(6L);
    }
}
