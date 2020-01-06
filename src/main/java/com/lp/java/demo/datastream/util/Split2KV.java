package com.lp.java.demo.datastream.util;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

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

