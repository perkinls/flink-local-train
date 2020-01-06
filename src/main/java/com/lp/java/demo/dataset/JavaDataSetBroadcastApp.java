package com.lp.java.demo.dataset;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;
import java.util.List;

/**
 * <p/>
 * <li>title: 广播变量</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2019/12/29 4:17 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description:
 * Flink支持广播变量，就是将数据广播到具体的taskmanager上，数据存储在内存中，这样可以减缓大量的shuffle操作
 * 1、准备需要广播的数据集
 * 2、广播数据
 * 3、获取广播数据
 * </li>
 */
public class JavaDataSetBroadcastApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 1. 初始化需要广播的数据集
        DataSet<Integer> toBroadcast = env.fromElements(1, 2, 3);
        DataSet<String> data = env.fromElements("a", "b");

        DataSet<String> result = data.map(new RichMapFunction<String, String>() {
            private List<Integer> mList = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 3. 获取广播的数据集
                Collection<Integer> broadcastSet = getRuntimeContext().getBroadcastVariable("broadcastSetName");
                this.mList = (List<Integer>) broadcastSet;
            }

            @Override
            public String map(String value) throws Exception {
                return value + "--->广播数据" + mList.toString();
            }
            // 2. withBroadcastSet 广播数据
        }).withBroadcastSet(toBroadcast, "broadcastSetName");

        result.print();
    }
}
