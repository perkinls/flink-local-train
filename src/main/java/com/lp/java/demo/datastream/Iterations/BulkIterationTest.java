package com.lp.java.demo.datastream.Iterations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

import java.util.ArrayList;
import java.util.List;

/*
    推荐文章：
    https://zhuanlan.zhihu.com/p/54802235
 */
public class BulkIterationTest {
    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Integer> list = new ArrayList<>();
        IterativeDataSet<Integer> initial= env.fromElements(1,2,3,4,5).iterate(10);

        DataSet<Integer> iteration= initial.map(new MapFunction<Integer, Integer>(){
            @Override
            public Integer map(Integer i) throws Exception{

                return i +1;
            }
        });

//        DataSet<Integer> count = initial.closeWith(iteration,iteration.filter(new FilterFunction<Integer>() {
//            @Override
//            public boolean filter(Integer value) throws Exception {
//                return value<5;
//            }
//        }));
        DataSet<Integer> count = initial.closeWith(iteration);
        count.print();

    }
}
