package com.lp.java.demo.datastream.processfunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * The implementation of the ProcessFunctionTimeout that maintains the count and timeouts
 */
public class CountWithTimeoutFunction extends ProcessFunction<Tuple2<String, String>, Tuple2<String, Long>> {

    /** The state that is maintained by this process function */
    private ValueState<CountWithTimestamp> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
    }

    @Override
    public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, Long>> out)
            throws Exception {

        // retrieve the current count
        CountWithTimestamp current = state.value();
        if (current == null) {
            current = new CountWithTimestamp();
            current.key = value.f0;
        }

        // update the state's count
        current.count++;

        // set the state's timestamp to the record's assigned event time timestamp
        current.lastModified = ctx.timestamp();

        // write the state back
        state.update(current);

        // schedule the next timer 60 seconds from the current event time
        //注册基于事件时间的timer
//        ctx.timerService().currentWatermark();
        ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);

        // 注册基于处理时间的timer
//        ctx.timerService().registerProcessingTimeTimer(current.lastModified + 60000);

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out)
            throws Exception {

        // get the state for the key that scheduled the timer
        CountWithTimestamp result = state.value();

        System.out.println("onTimer : "+result.key);
        // check if this is an outdated timer or the latest timer
        if (timestamp == result.lastModified + 60000) {
            System.out.println("onTimer timeout : "+result.key);

            // emit the state on timeout
            out.collect(new Tuple2<String, Long>(result.key, result.count));
        }
    }
}
