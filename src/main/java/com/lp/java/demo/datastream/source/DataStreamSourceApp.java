package com.lp.java.demo.datastream.source;

import com.lp.java.demo.commons.BaseStreamingEnv;
import com.lp.java.demo.commons.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.datastream.source.function.CustomNonParallelSourceFunction;
import com.lp.java.demo.datastream.source.function.CustomParallelSourceFunction;
import com.lp.java.demo.datastream.source.function.CustomRichParallelSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <p/>
 * <li>title: DataStream Source</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2019/12/29 5:00 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: Source</li>
 */
public class DataStreamSourceApp extends BaseStreamingEnv<Object> implements IBaseRunApp {


    @Override
    public void doMain() throws Exception {
        socketFunction(env);
//        nonParallelSourceFunction(env);
//        parallelSourceFunction(env);
//        richParallelSourceFunction(env);

        env.execute(JobConfigPo.jobNamePrefix + DataStreamSourceApp.class.getName());

    }


    public void richParallelSourceFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data = env.addSource(new CustomRichParallelSourceFunction()).setParallelism(2);
        data.print().setParallelism(1);
    }


    public void parallelSourceFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data = env.addSource(new CustomParallelSourceFunction()).setParallelism(2);
        data.print().setParallelism(1);
    }


    public void nonParallelSourceFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data = env.addSource(new CustomNonParallelSourceFunction());
        //.setParallelism(2);
        data.print().setParallelism(1);
    }

    public void socketFunction(StreamExecutionEnvironment env) {
        DataStreamSource<String> data = env.socketTextStream("localhost", 9999);
        data.print().setParallelism(1);
    }


}
