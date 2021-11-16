package com.lp.java.demo.datastream.transformation;

import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.datastream.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.datastream.source.function.CustomNonParallelSourceFunction;
import com.lp.java.demo.run.StreamingRunApp;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * <p/>
 * <li>title: DataStream Transformation</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2019/12/29 5:00 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: 转换算子</li>
 */
public class DataStreamTransformApp extends BaseStreamingEnv<Object> implements IBaseRunApp {
    private final static Logger log = LoggerFactory.getLogger(DataStreamTransformApp.class);

    @Override
    public void doMain() throws Exception {
        map(env);
        filterFunction(env);
        flatMap(env);
        connectFunction(env);
        unionFunction(env);
        splitSelectFunction(env);
        //reduceFunction(env);
        env.execute(JobConfigPo.jobNamePrefix + DataStreamTransformApp.class.getName());

    }


    public static void reduceFunction(StreamExecutionEnvironment env) {
        log.info("==============================Reduce Function==============================");

        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        source.flatMap((FlatMapFunction<String, String>) (value, out) -> {
            String[] splits = value.split(",");
            for (String word : splits) {
                out.collect(word);
            }
        }).map((MapFunction<String, Tuple2<String, Integer>>) value -> Tuple2.of(value, 1)).keyBy(x -> x.f0) // word相同的都会分到一个task中去执行
                .reduce((ReduceFunction<Tuple2<String, Integer>>) (value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1))
                .print();
    }

    public static void splitSelectFunction(StreamExecutionEnvironment env) {
        // Flink1.12中已经移除了过期的 DataStream#split 方法
    }


    public static void unionFunction(StreamExecutionEnvironment env) {
        log.info("==============================Union Function==============================");

        DataStreamSource<Long> data1 = env.addSource(new CustomNonParallelSourceFunction());
        DataStreamSource<Long> data2 = env.addSource(new CustomNonParallelSourceFunction());

        data1.union(data2).print().setParallelism(1);
    }

    public static void connectFunction(StreamExecutionEnvironment env) {
        log.info("==============================Connect Function==============================");

        DataStreamSource<Long> data1 = env.addSource(new CustomNonParallelSourceFunction());
        DataStreamSource<Long> data2 = env.addSource(new CustomNonParallelSourceFunction());

        ConnectedStreams<Long, Long> connect = data1.connect(data2);
    }


    public static void flatMap(StreamExecutionEnvironment env) {
        log.info("==============================flatMap Function==============================");
        ArrayList<Integer[]> list = new ArrayList<>();
        list.add(new Integer[]{1, 2});
        list.add(new Integer[]{3, 4});
        list.add(new Integer[]{5, 6});
        DataStreamSource<Integer[]> source = env.fromCollection(list);

        source.flatMap((FlatMapFunction<Integer[], Integer>) (value, out) -> {
            for (Integer split : value) {
                out.collect(split);
            }
        }).print();
    }

    public static void filterFunction(StreamExecutionEnvironment env) {
        log.info("==============================filter Function==============================");
        DataStreamSource<Long> data = env.addSource(new CustomNonParallelSourceFunction());
        data.map((MapFunction<Long, Long>) value -> {
            System.out.println("value = [" + value + "]");
            return value;
        }).filter((FilterFunction<Long>) value -> value % 2 == 0).print().setParallelism(1);

    }

    public static void map(StreamExecutionEnvironment env) {
        log.info("==============================map Function==============================");

        ArrayList<Integer> list = new ArrayList<>();
        list.add(1);  // map  * 2 = 2
        list.add(2);  // map  * 2 = 4
        list.add(3);  // map  * 2 = 6
        DataStreamSource<Integer> source = env.fromCollection(list);

        source.map((MapFunction<Integer, Integer>) value -> value * 2).print();

    }

}
