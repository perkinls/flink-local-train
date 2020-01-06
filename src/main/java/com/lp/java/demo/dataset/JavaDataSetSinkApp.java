package com.lp.java.demo.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

/**
 * <p/> DataSet  Sink </li>
 * <li>@author: li.pan</li>
 * <li>Date: 2019/12/29 15:10 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: </li>
 */
public class JavaDataSetSinkApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Integer> info = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            info.add(i);
        }

        String filePath = "file:///Users/lipan/workspace/flink_demo/flink-local-train/src/main/resources/sink/java/sink_test.txt";
        DataSource<Integer> data = env.fromCollection(info);
        data.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE);


        env.execute("JavaDataSetSinkApp");
    }
}
