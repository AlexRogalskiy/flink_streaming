package com.tomekl007.jobs.abandonedcart;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AbandonedCartNotifier implements WindowFunction<CartEvent, String, String, TimeWindow> {

    @Override
    public void apply(String userId,
                      TimeWindow window,
                      Iterable<CartEvent> input,
                      Collector<String> out) throws Exception {
       //todo implement
    }
}
