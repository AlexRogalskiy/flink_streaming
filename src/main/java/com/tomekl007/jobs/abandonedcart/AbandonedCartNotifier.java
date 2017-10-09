package com.tomekl007.jobs.abandonedcart;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class AbandonedCartNotifier implements WindowFunction<CartEvent, String, String, TimeWindow> {

    @Override
    public void apply(String userId,
                      TimeWindow window,
                      Iterable<CartEvent> input,
                      Collector<String> out) throws Exception {
        List<CartEvent> events = new LinkedList<>();
        for (CartEvent c : input) {
            events.add(c);
        }
        System.out.println("events: " + events);

        if (hasOnlyAddToCart(events)) {
            out.collect(userId);
        }

    }

    private boolean hasOnlyAddToCart(List<CartEvent> events) {
        return events.stream()
                .map(CartEvent::getTypeOfEvent)
                .filter(e -> e.equals(TypeOfEvents.BUY))
                .collect(Collectors.toList())
                .size() == 0
                &&
                events.stream()
                        .map(CartEvent::getTypeOfEvent)
                        .filter(e -> e.equals(TypeOfEvents.ADD_TO_CART))
                        .collect(Collectors.toList())
                        .size() >= 1;
    }
}
