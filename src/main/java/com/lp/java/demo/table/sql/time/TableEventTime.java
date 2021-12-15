package com.lp.java.demo.table.sql.time;

import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.commons.po.SensorPo;
import com.lp.java.demo.commons.po.WcPo;
import com.lp.java.demo.datastream.watermark.generator.BoundedOutOfOrderGenerator;
import com.lp.java.demo.table.sql.BaseTableEnv;
import net.sf.json.JSONObject;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author li.pan
 * @title Table 使用Process Time时间特性
 * @Date 2021/12/13
 */
public class TableEventTime extends BaseTableEnv<SensorPo> implements IBaseRunApp {
    private static final Logger log = LoggerFactory.getLogger(TableEventTime.class);


    @Override
    protected DataStream<SensorPo> mockDataStreamData() {
        return env.fromElements(
                new SensorPo("sensor_1", 1547718199L, 35.8),
                new SensorPo("sensor_6", 1547718201L, 15.4),
                new SensorPo("sensor_7", 1547718202L, 6.7),
                new SensorPo("sensor_10", 1547718205L, 38.1),
                new SensorPo("sensor_1", 1547718207L, 37.2),
                new SensorPo("sensor_1", 1547718212L, 33.5),
                new SensorPo("sensor_1", 1547718215L, 38.1)
        );
    }

    @Override
    public void doMain() throws Exception {

        SingleOutputStreamOperator<SensorPo> sensorPStream = mockDataStreamData().assignTimestampsAndWatermarks(
                WatermarkStrategy.<SensorPo>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<SensorPo>) (element, recordTimestamp) -> element.getTimestamp())
        );

        // No1. DataStream转指定时间字段  .rowtime  需要提前提取时间字段和watermark
        tableStreamEnv.createTemporaryView("event_time_table1", sensorPStream, $("id"), $("timestamp").rowtime(), $("temperature"));
        Table procTimeTable1 = tableStreamEnv.from("proc_time_table1");
        procTimeTable1.printSchema();

        log.info("====================================================================");
        // No2. 创建表的 DDL 中指定(代码逻辑仅供参考)
        tableStreamEnv.executeSql("" +
                "CREATE TABLE event_time_table1 (\n" +
                " id STRING,\n" +
                " timestamp INT,\n" +
                " temperature STRING,\n" +
                " rt AS TO_TIMESTAMP( FROM_UNIXTIME(timestamp) ),\n" + // 设置事件事件
                " watermark for rt as rt - interval '1' second"+ // 设置waterMark
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

        Table procTimeTable2 = tableStreamEnv.from("proc_time_table2");
        procTimeTable2.printSchema();
        env.execute();
    }


}
