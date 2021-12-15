package com.lp.java.demo.table.sql.source;

import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.commons.po.SensorPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.table.sql.BaseTableEnv;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

/**
 * @author li.pan
 * @title Table读取kafka数据源
 * @Date 2021/12/2
 */
public class KafkaSourceTable extends BaseTableEnv<SensorPo> implements IBaseRunApp {
    @Override
    protected Integer setDefaultParallelism() {
        return 1;
    }

    @Override
    public void doMain() throws Exception {

        tableStreamEnv.executeSql("CREATE TABLE KafkaSourceTable (\n" +
                "  senor_txt STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + KafkaConfigPo.sensorTopic + "',\n" +
                "  'properties.bootstrap.servers' = '" + KafkaConfigPo.bootstrapServers + "',\n" +
                "  'properties.group.id' = '" + KafkaConfigPo.groupId + "',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'raw'\n" + // 定义了如何把二进制数据映射到表的列上
                ")");
        // 使用ddl方式读取kafka数据
        // tableStreamEnv.executeSql("CREATE TABLE KafkaSourceTable (\n" +
        //         "  `id` BIGINT,\n" +
        //         "  `timestamp` TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
        //         "  `temperature` DOUBLE\n" +
        //         ") WITH (\n" +
        //         "  'connector' = 'kafka',\n" +
        //         "  'topic' = '" + KafkaConfigPo.sensorTopic + "',\n" +
        //         "  'properties.bootstrap.servers' = '" + KafkaConfigPo.bootstrapServers + "',\n" +
        //         "  'properties.group.id' = '" + KafkaConfigPo.groupId + "',\n" +
        //         "  'scan.startup.mode' = 'earliest-offset',\n" +
        //         "  'format' = 'csv'\n" + // 定义了如何把二进制数据映射到表的列上
        //         ")");
        Table kafkaSourceTable = tableStreamEnv.from("KafkaSourceTable");


        kafkaSourceTable.printSchema();

        tableStreamEnv.toAppendStream(kafkaSourceTable, Row.class).print();

        env.execute();


    }
}
