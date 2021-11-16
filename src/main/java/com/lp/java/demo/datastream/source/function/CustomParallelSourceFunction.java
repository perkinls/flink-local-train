package com.lp.java.demo.datastream.source.function;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;


/**
 * <p/>
 * <li>Description: 自定义并行流</li>
 * <li>@author: panli0226@sina.com</li>
 * <li>Date: 2019-04-15 13:28</li>
 * note: ParallelSourceFunction继承的SourceFunction接口，是怎样实现并行流的？
 * 在初始化DataStreamSource对象中，对是否是并行流进行了判断
 */
public class CustomParallelSourceFunction implements ParallelSourceFunction<Long> {
    private static final long serialVersionUID = -4479361562567758002L;
    boolean isRunning = true;
    long count = 1;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (true) {
            ctx.collect(count);
            count += 1;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
