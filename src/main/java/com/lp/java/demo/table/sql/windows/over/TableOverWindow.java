package com.lp.java.demo.table.sql.windows.over;

import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.commons.po.SensorPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.richfunction.RichMapSplit2Sensor;
import com.lp.java.demo.table.sql.BaseTableEnv;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @author li.pan
 * @title Table 滚动窗口
 * @Date 2021/12/13
 */
public class TableOverWindow extends BaseTableEnv<String> implements IBaseRunApp {
    @Override
    protected Integer setDefaultParallelism() {
        return 1;
    }

    @Override
    protected void setKafkaFromOffsets(FlinkKafkaConsumer<String> consumer) {
        consumer.setStartFromLatest();
    }

    private static final Logger log = LoggerFactory.getLogger(TableOverWindow.class);

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
                .window(Over.partitionBy($("id")).orderBy($("ts")).preceding(row(2)).as("ow")) //每10s统计一次，滚动事件窗口
                .select($("id"), $("ts"),$("id").count().over("ow"), $("temperature").avg().over("ow"), $("ow").start().as("w_start_time"), $("ow").end().as("w_end_time"));
        tableResult.printSchema();
        tableStreamEnv.toAppendStream(tableResult, Row.class).print("Table");

        log.info("======================================================================");

        tableStreamEnv.createTemporaryView("sensor_Table",sensorTable);
        Table sqlResult = tableStreamEnv.sqlQuery("select " +
                "   id,\n" +
                "   count(id) over ow\n"+
                "   avg(temperature) over ow\n" +
                "from sensor_Table\n" +
                "  window ow as (" +
                "   partition by id" +
                "   order by ts" +
                "   rows between 2 preceding and current row" +
                ")");
        sqlResult.printSchema();
        tableStreamEnv.toAppendStream(sqlResult, Row.class).print("SQL");

        env.execute();
    }
}
