package com.lp.java.demo.table.sql.udfs;

import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.table.sql.BaseTableEnv;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

/**
 * @author li.pan
 * @title 自定义表值聚合函数（UDTAGG）
 * @Date 2021/12/15
 * UDTAGG: 可以把一个表（一行或者多行，每行有一列或者多列）聚合成另一张表，结果中可以有多行多列。
 */
public class TableUseTableAggregateFunction extends BaseTableEnv<Object> implements IBaseRunApp {


    @Override
    public void doMain() throws Exception {
        tableStreamEnv.createTemporarySystemFunction("top2", new Top2());
        // 初始化表
        // Table tab = ...;

        // 使用函数
        // tab.groupBy("key")
        //         .flatAggregate("top2(a) as (v, rank)")
        //         .select("key, v, rank");

    }

    /**
     * Accumulator for Top2.
     */
    public static class Top2Accum {
        public Integer first;
        public Integer second;
    }

    /**
     * The top2 user-defined table aggregate function.
     */
    public class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Accum> {

        private static final long serialVersionUID = 2689876622113809391L;

        @Override
        public Top2Accum createAccumulator() {
            Top2Accum acc = new Top2Accum();
            acc.first = Integer.MIN_VALUE;
            acc.second = Integer.MIN_VALUE;
            return acc;
        }


        public void accumulate(Top2Accum acc, Integer v) {
            if (v > acc.first) {
                acc.second = acc.first;
                acc.first = v;
            } else if (v > acc.second) {
                acc.second = v;
            }
        }

        public void merge(Top2Accum acc, java.lang.Iterable<Top2Accum> iterable) {
            for (Top2Accum otherAcc : iterable) {
                accumulate(acc, otherAcc.first);
                accumulate(acc, otherAcc.second);
            }
        }

        public void emitValue(Top2Accum acc, Collector<Tuple2<Integer, Integer>> out) {
            // emit the value and rank
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, 1));
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, 2));
            }
        }
    }


}
