package com.lp.java.demo.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
/**
 * <p/>
 * <li>title: 批处理WordCount程序</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2019/12/29 12:43 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: 使用Java API来开发Flink的批处理应用程序.</li>
 */
public class BatchWcJavaApp {

    public static void main(String[] args) throws Exception {

        // resource -> input.txt
        String input = "file:///Users/lipan/workspace/flink_demo/flink-local-train/src/main/resources/data/input.txt";

        // step1：获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // step2：读取数据
        DataSource<String> text =  env.readTextFile(input);

        // step3: transform转换
        text.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = value.toLowerCase().split("\t");
                for(String token : tokens) {
                    if(token.length() > 0) {
                        collector.collect(new Tuple2<String,Integer>(token,1));
                    }
                }
            }
        }).groupBy(0).sum(1).print();

    }
}
