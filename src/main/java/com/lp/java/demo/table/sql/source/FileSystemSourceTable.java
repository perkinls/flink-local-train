package com.lp.java.demo.table.sql.source;

import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.commons.po.SensorPo;
import com.lp.java.demo.table.sql.BaseTableEnv;
import org.apache.flink.table.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @author li.pan
 * @title 文件作为flink sql输入源
 * @Date 2021/12/7
 * 官网参考地址: https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/connectors/table/filesystem/
 */
public class FileSystemSourceTable extends BaseTableEnv<Object> implements IBaseRunApp {
    private static final Logger log = LoggerFactory.getLogger(FileSystemSourceTable.class);

    @Override
    public void doMain() throws Exception {
        String filePath = Objects.requireNonNull(FileSystemSourceTable.class.getClassLoader().getResource("data"))
                .getPath();

        log.info("文件全路径: {}", filePath);
        // 通过sql读取外部文件作为sql数据源输入
        tableStreamEnv.executeSql("" +
                "CREATE TABLE FileSourceTable (\n" +
                "  id STRING,\n" +
                "  `timestamp` BIGINT,\n" +
                "  temperature DOUBLE\n" +
                ") WITH (\n" +
                "  'connector' = 'filesystem',          -- 必选: 指定连接器类型\n" +
                "  'path' = '" + filePath + "',         -- 必选: 指向目录的路径\n" +
                "  'format' = 'csv'                     -- 必选: 文件系统连接器需要指定格式，请查阅 表格式 部分以获取更多细节\n" +
                ")");

        Table kafkaSourceTable = tableStreamEnv.from("FileSourceTable");


        tableStreamEnv.toAppendStream(kafkaSourceTable, SensorPo.class).print();

        env.execute();

    }
}
