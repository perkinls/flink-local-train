package com.lp.java.demo.datastream.transformation;

import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.datastream.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.datastream.source.function.CustomNonParallelSourceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <p/>
 * <li>title: DataStream Transformation</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2019/12/29 5:00 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: 转换算子</li>
 */
public class DataStreamTransformApp extends BaseStreamingEnv<Object> implements IBaseRunApp {

    @Override
    public void doMain() throws Exception {
        // filterFunction(env);
        unionFunction(env);
        // splitSelectFunction(env);
        env.execute(JobConfigPo.jobNamePrefix + DataStreamTransformApp.class.getName());

    }


    public static void splitSelectFunction(StreamExecutionEnvironment env) {
        // Flink1.12中已经移除了过期的 DataStream#split 方法
    }


    public static void unionFunction(StreamExecutionEnvironment env) {

        DataStreamSource<Long> data1 = env.addSource(new CustomNonParallelSourceFunction());
        DataStreamSource<Long> data2 = env.addSource(new CustomNonParallelSourceFunction());

        data1.union(data2).print().setParallelism(1);
    }

    public static void connectFunction(StreamExecutionEnvironment env) {

        DataStreamSource<Long> data1 = env.addSource(new CustomNonParallelSourceFunction());
        DataStreamSource<Long> data2 = env.addSource(new CustomNonParallelSourceFunction());

        ConnectedStreams<Long, Long> connect = data1.connect(data2);
    }


    public static void filterFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data = env.addSource(new CustomNonParallelSourceFunction());
        data.map((MapFunction<Long, Long>) value -> {
            System.out.println("value = [" + value + "]");
            return value;
        }).filter((FilterFunction<Long>) value -> value % 2 == 0).print().setParallelism(1);

    }

}
