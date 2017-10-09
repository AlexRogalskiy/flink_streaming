package com.tomekl007.jobs.abandonedcart;

import flink.stream.contrib.DataStreamUtils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class AbandonedCartJobTest {


    @Test
    public void givenCartEvents_whenUserAddAndNotBuyInTimeWindow_thenShouldProduceAbandonedCartNotification() throws IOException {
        //given
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //when
        DataStream<CartEvent> input = env.fromElements(
                new CartEvent(
                        ZonedDateTime.now()
                                .withHour(1)
                                .withMinute(2)
                                .withSecond(2)
                                .truncatedTo(ChronoUnit.SECONDS),
                        "userId_1", TypeOfEvents.ADD_TO_CART),
                new CartEvent(
                        ZonedDateTime
                                .now()
                                .withHour(1)
                                .withMinute(2)
                                .withSecond(3)
                                .truncatedTo(ChronoUnit.SECONDS),
                        "userId_1",
                        TypeOfEvents.BUY),
                new CartEvent(ZonedDateTime
                        .now()
                        .withHour(1)
                        .withMinute(2)
                        .withSecond(4)
                        .truncatedTo(ChronoUnit.SECONDS), "userId_2",
                        TypeOfEvents.ADD_TO_CART)
        ).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<CartEvent>() {
            @Override
            public long extractAscendingTimestamp(CartEvent event) {
                return event.getEventTime().toInstant().toEpochMilli();
            }
        });

        SingleOutputStreamOperator<String> abandonedCartNotifications
                = input
                .keyBy(new KeySelector<CartEvent, String>() {
                    @Override
                    public String getKey(CartEvent value) throws Exception {
                        return value.getUserId();
                    }
                })
                .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
                .apply(new AbandonedCartNotifier());

        //then
        List<String> result = DataStreamUtils.collect(abandonedCartNotifications);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0)).isEqualTo("userId_2");
    }


    @Test
    public void givenCartEvents_whenUserAddAndBuyInTimeWindow_thenShouldNOTProduceAbandonedCartNotification() throws IOException {
        //given
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //when
        DataStream<CartEvent> input = env.fromElements(
                new CartEvent(
                        ZonedDateTime.now()
                                .withHour(1)
                                .withMinute(2)
                                .withSecond(2)
                                .truncatedTo(ChronoUnit.SECONDS),
                        "userId_1", TypeOfEvents.ADD_TO_CART),
                new CartEvent(
                        ZonedDateTime
                                .now()
                                .withHour(1)
                                .withMinute(2)
                                .withSecond(3)
                                .truncatedTo(ChronoUnit.SECONDS),
                        "userId_1",
                        TypeOfEvents.BUY)
        ).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<CartEvent>() {
            @Override
            public long extractAscendingTimestamp(CartEvent event) {
                return event.getEventTime().toInstant().toEpochMilli();
            }
        });

        SingleOutputStreamOperator<String> abandonedCartNotifications
                = input
                .keyBy(new KeySelector<CartEvent, String>() {
                    @Override
                    public String getKey(CartEvent value) throws Exception {
                        return value.getUserId();
                    }
                })
                .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
                .apply(new AbandonedCartNotifier());

        //then
        List<String> result = DataStreamUtils.collect(abandonedCartNotifications);
        assertThat(result.size()).isEqualTo(0);
    }

    @Test
    public void givenCartEvents_whenUserAddAndBuyBuyInSeparateSession_thenShouldProduceAbandonedCartNotification() throws IOException {
        //given
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //when
        DataStream<CartEvent> input = env.fromElements(
                new CartEvent(
                        ZonedDateTime.now()
                                .withHour(1)
                                .withMinute(2)
                                .withSecond(2)
                                .truncatedTo(ChronoUnit.SECONDS),
                        "userId_1", TypeOfEvents.ADD_TO_CART),
                new CartEvent(
                        ZonedDateTime
                                .now()
                                .withHour(1)
                                .withMinute(2)
                                .withSecond(23)
                                .truncatedTo(ChronoUnit.SECONDS),
                        "userId_1",
                        TypeOfEvents.BUY)
        ).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<CartEvent>() {
            @Override
            public long extractAscendingTimestamp(CartEvent event) {
                return event.getEventTime().toInstant().toEpochMilli();
            }
        });

        SingleOutputStreamOperator<String> abandonedCartNotifications
                = input
                .keyBy(new KeySelector<CartEvent, String>() {
                    @Override
                    public String getKey(CartEvent value) throws Exception {
                        return value.getUserId();
                    }
                })
                .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
                .apply(new AbandonedCartNotifier());

        //then
        List<String> result = DataStreamUtils.collect(abandonedCartNotifications);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0)).isEqualTo("userId_1");
    }

}
