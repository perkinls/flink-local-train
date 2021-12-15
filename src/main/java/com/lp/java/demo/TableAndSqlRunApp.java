package com.lp.java.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author li.pan
 * @title Table语法或者sql语法启动类
 * @Date 2021/11/24
 */
public class TableAndSqlRunApp {

    private final static Logger log = LoggerFactory.getLogger(TableAndSqlRunApp.class);

    public static void main(String[] args) {
        try {
            // WordCount 程序
            // new WordCountTable().doMain();
            // new WordCountSql().doMain();
            // new StreamSQLExample().doMain();

            // 创建Table的方式
            // new CreateTable().doMain();

            // Source 读取数据
            // new FileSystemSourceTable().doMain();
            // new KafkaSourceTable().doMain();
            // new DataGenSourceTable().doMain();

            // Sink 写入外部系统
            // new TableSinkKafka().doMain();
            // new TableSinkEs().doMain();

            // Time 时间属性
            // new TableProcTime().doMain();
            // new TableEventTime().doMain();

            // Window 窗口操作
            // new TableTubmleWindow().doMain();
            // new TableOverWindow().doMain();

            // 自定义udf函数
            // new TableUseScalarFunction().doMain();
            // new TableUseTableFunction().doMain();
            // new TableUseAggregateFunction().doMain(); // 未模拟数据

        } catch (Exception e) {
            log.error("程序处理错误,{}", e.getMessage());
            e.printStackTrace();
        }

    }
}
