package com.lp.java.demo.datastream.Iterations;

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/*
    推荐阅读：
    https://zhuanlan.zhihu.com/p/54957049
 */
public class IterateDeltaTest {
    public static void main(String[] args){
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Integer> data = new ArrayList<>();
        data.add(1);
        data.add(2);
        data.add(2);
        data.add(3);
        data.add(3);
        data.add(3);
        data.add(4);
        data.add(4);
        data.add(4);
        data.add(4);
        data.add(5);
        data.add(5);
        data.add(5);
        data.add(5);
        data.add(5);

        Collections.shuffle(data);

        DataSet<Tuple2<Integer, Integer>> initialSolutionSet = env.fromCollection(data).map(new TupleMakerMap());
        MapOperator<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> workset = initialSolutionSet.map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
                return value;
            }
        });

        DeltaIteration<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> iteration = initialSolutionSet.iterateDelta(
                workset, 20, 0);

        // register aggregator
        LongSumAggregator aggr = new LongSumAggregator();
        iteration.registerAggregator("count.negative.elements", aggr);

        DataSet<Tuple2<Integer, Integer>> updatedDs = iteration.getWorkset().map(new AggregateMapDelta());

        DataSet<Tuple2<Integer, Integer>> solutionsetDeltDataset = updatedDs.
                join(iteration.getSolutionSet())
                .where(0)
                .equalTo(0)
                .flatMap(new UpdateFilter());

        FilterOperator<Tuple2<Integer, Integer>> worksetDeltDataset = solutionsetDeltDataset.filter(
                new FilterFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple2<Integer, Integer> value) throws Exception {
                        return value.f1 > 10;
                    }
                }
        );
        DataSet<Tuple2<Integer, Integer>> iterationRes = iteration.closeWith(solutionsetDeltDataset, worksetDeltDataset);
        List<Integer> result = null;
        try {
            result = iterationRes.map(new ProjectSecondMapper()).collect();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Collections.sort(result);
        for(int i : result){
            System.out.println(i);
        }
    }
    private static final class  TupleMakerMap extends RichMapFunction<Integer, Tuple2<Integer, Integer>> {

        private Random rnd;

        @Override
        public void open(Configuration parameters){
            rnd = new Random(0xC0FFEBADBEEFDEADL + getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public Tuple2<Integer, Integer> map(Integer value) {
            Integer nodeId = rnd.nextInt(100000);
            return new Tuple2<>(nodeId, value);
        }

    }
    private static final class AggregateMapDelta extends RichMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

        private LongSumAggregator aggr;
        private LongValue previousAggr;
        private int superstep;

        @Override
        public void open(Configuration conf) {
            aggr = getIterationRuntimeContext().getIterationAggregator("count.negative.elements");
            superstep = getIterationRuntimeContext().getSuperstepNumber();

            if (superstep > 1) {
                previousAggr = getIterationRuntimeContext().getPreviousIterationAggregate("count.negative.elements");
            }

        }

        @Override
        public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) {
            // count the elements that are equal to the superstep number
            if (value.f1 == superstep) {
                aggr.aggregate(1L);
            }
            return value;
        }
    }

    private static final class UpdateFilter extends RichFlatMapFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>,
            Tuple2<Integer, Integer>> {

        private int superstep;

        @Override
        public void open(Configuration conf) {
            superstep = getIterationRuntimeContext().getSuperstepNumber();
        }

        @Override
        public void flatMap(Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> value,
                            Collector<Tuple2<Integer, Integer>> out) {

            if (value.f0.f1  > superstep) {
                out.collect(value.f0);
            }
        }
    }
    private static final class ProjectSecondMapper extends RichMapFunction<Tuple2<Integer, Integer>, Integer> {

        @Override
        public Integer map(Tuple2<Integer, Integer> value) {
            return value.f1;
        }
    }

}
