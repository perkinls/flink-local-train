package com.lp.java.demo.table.sql.sink;

import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.table.sql.BaseTableEnv;
import org.apache.flink.table.api.Table;
import sun.management.Sensor;

/**
 * @author li.pan
 * @title Table Sink输出到kafka
 * @Date 2021/12/7
 */
public class TableSinkKafka extends BaseTableEnv<Sensor> implements IBaseRunApp {
    @Override
    public void doMain() throws Exception {
        tableStreamEnv.executeSql("" +
                "CREATE TABLE DataGenSourceTable (\n" +
                " f_sequence INT,\n" +
                " f_random INT,\n" +
                " f_random_str STRING,\n" +
                " ts AS localtimestamp,\n" +
                " WATERMARK FOR ts AS ts\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='5',                -- 每秒生成的行数，用以控制数据发出速率。\n" +
                " 'fields.f_sequence.kind'='sequence',  -- 指定f_sequence字段的生成器。可以是 'sequence' 或 'random'。\n" +
                " 'fields.f_sequence.start'='1',        -- 序列生成器的起始值\n" +
                " 'fields.f_sequence.end'='1000',       -- 序列生成器的结束值\n" +
                " 'fields.f_random.min'='1',            -- 随机生成器的最小值\n" +
                " 'fields.f_random.max'='1000',         -- 随机生成器的最大值\n" +
                " 'fields.f_random_str.length'='10'     -- 随机生成器生成字符的长度，适用于 char、varchar、string。\n" +
                ")");

        tableStreamEnv.from("DataGenSourceTable");
        Table queryTale = tableStreamEnv.sqlQuery("select f_sequence,f_random,f_random_str from DataGenSourceTable");


        tableStreamEnv.executeSql("CREATE TABLE TableKafkaSink (\n" +
                " f_sequence INT,\n" +
                " f_random INT,\n" +
                " f_random_str STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'flink_table_sink_kafka',\n" +
                "  'properties.bootstrap.servers' = '" + KafkaConfigPo.bootstrapServers + "',\n" +
                "  'properties.group.id' = '" + KafkaConfigPo.groupId + "',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv'\n" + // 定义了如何把二进制数据映射到表的列上
                ")");

        queryTale.printSchema();
        queryTale.executeInsert("TableKafkaSink");
        env.execute();
    }
}
