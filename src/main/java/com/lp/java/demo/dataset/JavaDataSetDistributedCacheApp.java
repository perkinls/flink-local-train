package com.lp.java.demo.dataset;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * <p/> DataSet 分布式缓存 </li>
 * <li>@author: li.pan</li>
 * <li>Date: 2019/12/29 16:10 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: </li>
 */
public class JavaDataSetDistributedCacheApp {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env =  ExecutionEnvironment.getExecutionEnvironment();
        String filePath = "file:///Users/lipan/workspace/flink_demo/flink-local-train/src/main/resources/sink/java/cache.txt";

        // step1: 注册一个本地/HDFS文件
        env.registerCachedFile(filePath, "lp-java-dc");

        DataSource<String> data = env.fromElements("hadoop","spark","flink","pyspark","storm");

        data.map(new RichMapFunction<String, String>() {

            List<String> list = new ArrayList<String>();
            // step2：在open方法中获取到分布式缓存的内容即可
            @Override
            public void open(Configuration parameters) throws Exception {
                File file = getRuntimeContext().getDistributedCache().getFile("lp-java-dc");
                List<String> lines = FileUtils.readLines(file);
                for(String line : lines) {
                    list.add(line);
                    System.out.println("line = [" + line + "]");
                }
            }

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();

    }
}
