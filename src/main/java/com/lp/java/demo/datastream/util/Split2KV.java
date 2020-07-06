package com.lp.java.demo.datastream.util;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * <p/> 
 * <li>Description: kv解析</li>
 * <li>@author: panli0226@sina.com</li> 
 * <li>Date: 2020-01-07 21:35</li> 
 */
public class Split2KV extends RichMapFunction<String, Tuple2<String,Long>> {
    private static final long serialVersionUID = 1180234853172462378L;
    @Override
    public Tuple2<String,Long> map(String event) throws Exception {
        String[] split = event.split(" ");
        return new Tuple2<>(split[0],Long.valueOf(split[1]));
    }
    @Override
    public void open(Configuration parameters) throws Exception {
    }
}

