package com.lp.java.demo.table.sql.sink;

import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.table.sql.BaseTableEnv;
import com.lp.java.demo.table.sql.source.FileSystemSourceTable;
import org.apache.flink.table.api.Table;
import sun.management.Sensor;

import java.util.Objects;

/**
 * @author li.pan
 * @title Table Sink输出到文件
 * @Date 2021/12/7
 */
public class TableSinkFile extends BaseTableEnv<Sensor> implements IBaseRunApp {
    @Override
    public void doMain() throws Exception {
        String filePath = Objects.requireNonNull(FileSystemSourceTable.class.getClassLoader().getResource("sink"))
                .getPath();
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

        tableStreamEnv.executeSql("CREATE TABLE TableFileSink (\n" +
                " f_sequence INT,\n" +
                " f_random INT,\n" +
                " f_random_str STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'filesystem',                    -- 必选: 指定连接器类型\n" +
                "  'path' = '"+filePath+"',                       -- 必选: 指向目录的路径\n" +
                "  'format' = 'json',                             -- 必选: 文件系统连接器需要指定格式\n" +
                "  'sink.shuffle-by-partition.enable' = 'false',  -- 可选: 该选项开启了在 sink 阶段通过动态分区字段来 shuffle 数据，该功能可以大大减少文件系统 sink 的文件数，但可能会导致数据倾斜，默认值是 false.\n" + // 定义了如何把二进制数据映射到表的列上
                ")");

        queryTale.printSchema();
        queryTale.executeInsert("TableFileSink");
        env.execute();
    }
}
