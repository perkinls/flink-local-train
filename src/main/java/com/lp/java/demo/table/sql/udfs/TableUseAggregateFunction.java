package com.lp.java.demo.table.sql.udfs;

import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.commons.po.WcPo;
import com.lp.java.demo.table.sql.BaseTableEnv;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.Iterator;

/**
 * @author li.pan
 * @title 自定义聚合函数
 * @Date 2021/12/15
 * 自定义聚合函数（UDAGG）是把一个表（一行或者多行，每行可以有一列或者多列）聚合成一个标量值。
 * 官网地址: https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/table/functions/udfs/
 *
 *
 * 需求获取word中的最大值 >>>>>>>
 *     它需要一个 accumulator，它是一个数据结构，存储了聚合的中间结果。通过调用 AggregateFunction 的 createAccumulator() 方法创建一个空的 accumulator。
 *     接下来，对于每一行数据，会调用 accumulate() 方法来更新 accumulator。当所有的数据都处理完了之后，通过调用 getValue 方法来计算和返回最终的结果。
 */
public class TableUseAggregateFunction extends BaseTableEnv<WcPo> implements IBaseRunApp {

    @Override
    public void doMain() throws Exception {

        // 为了计算加权平均值，accumulator 需要存储加权总和以及数据的条数。

        // 注册函数
        tableStreamEnv.createTemporarySystemFunction("wAvg", new WeightedAvg());

        // 使用函数
        tableStreamEnv.sqlQuery("SELECT user, wAvg(points, level) AS avgPoints FROM userScores GROUP BY user");

    }


    /**
     * Accumulator for WeightedAvg.
     */
    public static class WeightedAvgAccum {
        public long sum = 0;
        public int count = 0;
    }

    /**
     * Weighted Average user-defined aggregate function.
     */
    public static class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccum> {

        private static final long serialVersionUID = 2365865314304787883L;

        @Override
        public WeightedAvgAccum createAccumulator() {
            return new WeightedAvgAccum();
        }

        @Override
        public Long getValue(WeightedAvgAccum acc) {
            if (acc.count == 0) {
                return null;
            } else {
                return acc.sum / acc.count;
            }
        }

        public void accumulate(WeightedAvgAccum acc, long iValue, int iWeight) {
            acc.sum += iValue * iWeight;
            acc.count += iWeight;
        }

        public void retract(WeightedAvgAccum acc, long iValue, int iWeight) {
            acc.sum -= iValue * iWeight;
            acc.count -= iWeight;
        }

        public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
            Iterator<WeightedAvgAccum> iter = it.iterator();
            while (iter.hasNext()) {
                WeightedAvgAccum a = iter.next();
                acc.count += a.count;
                acc.sum += a.sum;
            }
        }

        public void resetAccumulator(WeightedAvgAccum acc) {
            acc.count = 0;
            acc.sum = 0L;
        }
    }
}
