package com.lp.java.demo.table.sql.source;

import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.table.sql.BaseTableEnv;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author li.pan
 * @title DataGen 连接器允许按数据生成规则进行读取
 * @Date 2021/12/7
 */
public class DataGenSourceTable extends BaseTableEnv<Object> implements IBaseRunApp {
    private static final Logger log = LoggerFactory.getLogger(DataGenSourceTable.class);

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

        Table dataGenSourceTable = tableStreamEnv.from("DataGenSourceTable");
        tableStreamEnv.toAppendStream(dataGenSourceTable, Row.class).print();

        env.execute();

    }
}
