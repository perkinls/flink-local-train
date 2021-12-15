package com.lp.java.demo.table.sql.time;

import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.commons.po.SensorPo;
import com.lp.java.demo.commons.po.WcPo;
import com.lp.java.demo.table.sql.BaseTableEnv;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author li.pan
 * @title Table 使用Process Time时间特性
 * @Date 2021/12/13
 */
public class TableProcTime extends BaseTableEnv<WcPo> implements IBaseRunApp {
    private static final Logger log = LoggerFactory.getLogger(TableProcTime.class);

    @Override
    protected DataStream<WcPo>mockDataStreamData() {
        return env.fromElements(
                new WcPo("Hello", 1),
                new WcPo("Ciao", 1),
                new WcPo("Hello", 1)
        );
    }

    @Override
    public void doMain() throws Exception {

        // No1. DataStream转指定时间字段
        // proctime 属性只能通过附加逻辑字段，来扩展物理 schema。因此，只能在 schema 定义的末尾定义它。
        tableStreamEnv.createTemporaryView("proc_time_table1", mockDataStreamData(), $("word"), $("frequency"), $("pt").proctime());
        Table procTimeTable1 = tableStreamEnv.from("proc_time_table1");
        procTimeTable1.printSchema();

        log.info("====================================================================");
        // No2. 创建表的 DDL 中指定
        tableStreamEnv.executeSql("" +
                "CREATE TABLE proc_time_table2 (\n" +
                " f_sequence INT,\n" +
                " f_random INT,\n" +
                " f_random_str STRING,\n" +
                " pt AS PROCTIME()\n" + // -- PROCTIME()处理时间
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
