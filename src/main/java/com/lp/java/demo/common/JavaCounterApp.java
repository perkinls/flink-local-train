package com.lp.java.demo.common;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

/**
 * <p/>
 * <li>title: flink 计数器</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2019/12/29 2:59 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description:
 * Java实现通过一个add操作累加最终的结果，在job执行后可以获取最终结果
 * </li>
 */
public class JavaCounterApp {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> data = env.fromElements("hadoop","spark","flink","pyspark","storm");

        DataSet<String> info = data.map(new RichMapFunction<String, String>() {

            LongCounter counter = new LongCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                getRuntimeContext().addAccumulator("ele-counts-java", counter);
            }

            @Override
            public String map(String value) throws Exception {
                counter.add(1);
                return value;
            }
        });

        String filePath = "file:///Users/lipan/workspace/flink_demo/flink-local-train/src/main/resources/sink/java/";
        info.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE).setParallelism(2);
        JobExecutionResult jobResult = env.execute("CounterApp");

        // step3: 获取计数器
        long num = jobResult.getAccumulatorResult("ele-counts-java");

        System.out.println("num = [" + num + "]");
    }
}
