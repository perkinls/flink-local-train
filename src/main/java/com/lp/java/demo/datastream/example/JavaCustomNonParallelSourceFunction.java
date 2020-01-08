package com.lp.java.demo.datastream.example;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * <p/> 
 * <li>Description: 自定义source非并行</li>
 * <li>@author: panli0226@sina.com</li> 
 * <li>Date: 2019-04-15 13:21</li> 
 */
public class JavaCustomNonParallelSourceFunction implements SourceFunction<Long> {
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
