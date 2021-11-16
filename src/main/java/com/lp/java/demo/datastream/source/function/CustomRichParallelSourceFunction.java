package com.lp.java.demo.datastream.source.function;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p/>
 * <li>Description: 自定义source并行（带生命周期）</li>
 * <li>@author: panli0226@sina.com</li>
 * <li>Date: 2019-04-15 13:35</li>
 */
public class CustomRichParallelSourceFunction extends RichParallelSourceFunction<Long> {
    private static final Logger log = LoggerFactory.getLogger(CustomRichParallelSourceFunction.class);
    private static final long serialVersionUID = 3146129818140386079L;
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
    public void open(Configuration parameters) throws Exception {
        log.info("==============================【生命周期 -> open】==============================");

        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        log.info("==============================【生命周期 -> close】==============================");
        super.close();
    }

    @Override
    public void cancel() {
        log.info("==============================【生命周期 -> cancel】==============================");
        isRunning = false;
    }
}
