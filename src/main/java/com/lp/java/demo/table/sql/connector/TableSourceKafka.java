package com.lp.java.demo.table.sql.connector;

import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.commons.po.SensorPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.table.sql.BaseTableEnv;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author li.pan
 * @title Table读取kafka数据源
 * @Date 2021/12/2
 */
public class TableSourceKafka extends BaseTableEnv<SensorPo> implements IBaseRunApp {
    @Override
    protected Integer setDefaultParallelism() {
        return 1;
    }

    @Override
    protected void setKafkaFromOffsets(FlinkKafkaConsumer<SensorPo> consumer) {
        super.setKafkaFromOffsets(consumer);
    }

    @Override
    public void doMain() throws Exception {
        tableStreamEnv.executeSql("CREATE TABLE KafkaTable (\n" +
                "  `id` BIGINT,\n" +
                "  `temperature` BIGINT,\n" +
                "  `timestamp` TIMESTAMP(3) METADATA FROM 'timestamp'\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + KafkaConfigPo.sensorTopic + "',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")");



//        tableStreamEnv.connect();


    }
}
