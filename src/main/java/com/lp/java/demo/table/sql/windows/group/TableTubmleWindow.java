package com.lp.java.demo.table.sql.windows.group;

import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.commons.po.SensorPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.richfunction.RichMapSplit2Sensor;
import com.lp.java.demo.table.sql.BaseTableEnv;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @author li.pan
 * @title Table 滚动窗口
 * @Date 2021/12/13
 */
public class TableTubmleWindow extends BaseTableEnv<String> implements IBaseRunApp {
    @Override
    protected Integer setDefaultParallelism() {
        return 1;
    }

    @Override
    protected void setKafkaFromOffsets(FlinkKafkaConsumer<String> consumer) {
        consumer.setStartFromLatest();
    }

    private static final Logger log = LoggerFactory.getLogger(TableTubmleWindow.class);

    @Override
    public void doMain() throws Exception {
        FlinkKafkaConsumer<String> kafkaConsumer
                = getKafkaConsumer(KafkaConfigPo.sensorTopic, new SimpleStringSchema());

        // 获取DataStream
        SingleOutputStreamOperator<SensorPo> sensorSource = env
                .addSource(kafkaConsumer)
                .map(new RichMapSplit2Sensor());
        sensorSource.print("sensorSource");

        // DataStream -> Table
        Table sensorTable = tableStreamEnv.fromDataStream(sensorSource, $("id"), $("timestamp").rowtime().as("ts"), $("temperature"));

        // 执行Table的Window操作
        Table tableResult = sensorTable
                .window(Tumble.over(lit(10).seconds()).on($("ts")).as("w")) //每10s统计一次，滚动事件窗口
                .groupBy($("w"), $("id"))
                .select($("id"), $("id").count(), $("temperature").avg(), $("w").start().as("w_start_time"), $("w").end().as("w_end_time"));
        tableResult.printSchema();
        tableStreamEnv.toAppendStream(tableResult, Row.class).print("Table");

        log.info("======================================================================");

        tableStreamEnv.createTemporaryView("sensor_Table",sensorTable);
        Table sqlResult = tableStreamEnv.sqlQuery("select " +
                "id,\n" +
                "count(id) as id_cnt,\n" +
                "avg(temperature) as temp_avg,\n" +
                "tumble_start(ts,interval '10' second),\n" +
                "tumble_end(ts,interval '10' second)\n" +
                "from sensor_Table\n" +
                "group by id,\n" +
                "tumble(ts,interval '10' second)");
        sqlResult.printSchema();
        tableStreamEnv.toAppendStream(sqlResult, Row.class).print("SQL");

        env.execute();
    }
}
