package com.tomekl007.jobs.deduplication;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class DeduplicateFilter extends RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, String>> {

    static final ValueStateDescriptor<Boolean> descriptor
            = new ValueStateDescriptor<>("seen", Boolean.class, false);
    private ValueState<Boolean> operatorState;

    @Override
    public void open(Configuration configuration) {
        operatorState = this.getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<String, String> value, Collector<Tuple2<String, String>> out) throws Exception {
      //todo implement deduplication using operatorState ( it is keyed by the unique message id )
    }
}