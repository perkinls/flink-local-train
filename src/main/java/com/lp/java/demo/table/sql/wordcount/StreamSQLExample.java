package com.lp.java.demo.table.sql.wordcount;

import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.commons.po.OrderPo;
import com.lp.java.demo.table.sql.BaseTableEnv;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author li.pan
 * @title 流转化为SQL操作
 * @Date 2021/12/2
 */
public class StreamSQLExample extends BaseTableEnv implements IBaseRunApp {
    @Override
    public void doMain() throws Exception {

        DataStream<OrderPo> orderA = env.fromCollection(Arrays.asList(
                new OrderPo(1L, "beer", 3),
                new OrderPo(1L, "diaper", 4),
                new OrderPo(3L, "rubber", 2)));

        DataStream<OrderPo> orderB = env.fromCollection(Arrays.asList(
                new OrderPo(2L, "pen", 3),
                new OrderPo(2L, "rubber", 3),
                new OrderPo(4L, "beer", 1)));

        // convert DataStream to Table
        Table tableA = tableStreamEnv.fromDataStream(orderA, $("user"), $("product"),$("amount"));
        // register DataStream as Table
        tableStreamEnv.createTemporaryView("OrderB", orderB,  $("user"), $("product"),$("amount"));

        // union the two tables
        Table result = tableStreamEnv.sqlQuery("SELECT * FROM " + tableA + " WHERE amount > 2 UNION ALL " +
                "SELECT * FROM OrderB WHERE amount < 2");

        tableStreamEnv.toAppendStream(result, OrderPo.class).print();

        env.execute();
    }
}
