package com.lp.java.demo.table.sql;

import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.table.sql.wordcount.WordCountTable;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author li.pan
 * @title Table&SQL运行环境
 * @Date 2021/11/25
 */
public abstract class BaseTableEnv<T> extends BaseStreamingEnv<T> {

    /**
     * Table 流执行环境
     */
    protected StreamTableEnvironment tableStreamEnv = getStreamTableEnv();

    /**
     * Table 批执行环境(old)
     */
    protected BatchTableEnvironment tableOldBatchEnv = getOldBatchTableEnv();

    /**
     * Table 批执行环境(blink)
     */
    protected TableEnvironment tableBlinkBatchEnv = getBlinkBatchTableEnv();

    protected static ExecutionEnvironment batchEnv =getExecutionEnvironment();

    /**
     * 批执行环境
     *
     * @return BatchTableEnvironment
     */
    private static ExecutionEnvironment getExecutionEnvironment() {
        return ExecutionEnvironment.getExecutionEnvironment();
    }

    /**
     * 获取Table中流方式的执行环境
     *
     * @return StreamTableEnvironment
     */
    private StreamTableEnvironment getStreamTableEnv() {
        return StreamTableEnvironment.create(env, envSettings());
    }

    /**
     * 基于Old版本planner的执行环境
     *
     * @return BatchTableEnvironment
     */
    private BatchTableEnvironment getOldBatchTableEnv() {
        return BatchTableEnvironment.create(batchEnv);
    }



    /**
     * 基于Blink版本planner的执行环境
     *
     * @return BatchTableEnvironment
     */
    private TableEnvironment getBlinkBatchTableEnv() {
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        return TableEnvironment.create(blinkBatchSettings);
    }


    /**
     * 指定EnvironmentSettings各个参数
     *
     * @return EnvironmentSettings
     */
    private EnvironmentSettings envSettings() {
        EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance();
        setPlanner(builder);
        setRuntimeMode(builder);
        return builder.build();
    }


    /**
     * 设置planner方式
     *
     * @param builder builder
     */
    protected void setPlanner(EnvironmentSettings.Builder builder) {
        // builder.useOldPlanner();
        builder.useBlinkPlanner();
    }

    /**
     * 设置流还是批处理方式
     *
     * @param builder builder
     */
    protected void setRuntimeMode(EnvironmentSettings.Builder builder) {
        //builder.inBatchMode();
        builder.inStreamingMode();
    }

    /**
     * 批处理默认加载的数据集
     * tips: 子类复写该方法,重新模拟批处理环境数据集
     *
     */
    protected DataSet<T> mockDataSetData() {
        return null;
    }



}
