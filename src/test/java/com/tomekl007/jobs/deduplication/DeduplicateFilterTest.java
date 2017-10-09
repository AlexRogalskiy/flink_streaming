package com.tomekl007.jobs.deduplication;

import flink.stream.contrib.DataStreamUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class DeduplicateFilterTest {

    @Test
    public void givenStreamWithDuplicates_whenDuplicateArrive_thenShouldFilterItOut() throws IOException {
        //given
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String uniqueIdOfDuplicate = UUID.randomUUID().toString();

        //when
        DataStream<Tuple2<String, String>> input = env.fromElements(
                Tuple2.of(uniqueIdOfDuplicate, "value_2"),
                Tuple2.of(UUID.randomUUID().toString(), "value"),
                Tuple2.of(uniqueIdOfDuplicate, "value_2"));

        SingleOutputStreamOperator<Tuple2<String, String>> dedupliacte
                = input.keyBy(0).flatMap(new DeduplicateFilter());

        //then
        List<Tuple2<String, String>> result = DataStreamUtils.collect(dedupliacte);
        assertThat(result.size()).isEqualTo(2);

    }

}