package com.lp.java.demo.table.sql.windows;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author:cbxg
 * @date:2021/4/10
 * @description: over窗口 sql语法示例
 */
public class OverWindowSql {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("create table sensor (" +
                "id string ," +
                "ts bigint ," +
                "vc int ," +
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')),"+
                "watermark for t as t - interval '30' second " +
                ") with(" +
                "'connector' = 'filesystem'," +
                "'format' = 'csv'," +
                "'path' = 'D:\\gitProjects\\flink_sql_tutorials\\src\\main\\resources\\sensor.csv'" +
                ")");

//        tableEnv
//                .sqlQuery(
//                        "select " +
//                                "id," +
//                                "t," +
//                                "vc," +
//                                "sum(vc) over(partition by id order by t rows between 1 PRECEDING and current row)"
//                                + "from sensor"
//                )
//                .execute()
//                .print();

        tableEnv
                .sqlQuery(
                        "select " +
                                "id," +
                                "vc," +
                                "count(vc) over w, " +
                                "sum(vc) over w " +
                                "from sensor " +
                                "window w as (partition by id order by t rows between 1 PRECEDING and current row)"
                )
                .execute()
                .print();


//        tableEnv.sqlQuery("select id,vc,sum(vc)," +
//                "sum(vc) over(partition by id order by t rows between 1 PRECEDING and current row)" +
//                " from sensor")
//                .execute().print();

    }
}
