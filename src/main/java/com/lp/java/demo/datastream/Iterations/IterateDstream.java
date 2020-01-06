package com.lp.java.demo.datastream.Iterations;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
    原理文章介绍
    https://zhuanlan.zhihu.com/p/54802235
    https://zhuanlan.zhihu.com/p/54957049
    https://zhuanlan.zhihu.com/p/59376083
 */
public class IterateDstream {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStream<Long> someIntegers = env.generateSequence(0, 1000);
        // 创建迭代流
        IterativeStream<Long> iteration = someIntegers.iterate();
        // 增加处理逻辑，对元素执行减一操作。
        DataStream<Long> minusOne = iteration.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println(value);
                return value - 1 ;
            }
        });

//        // 获取要进行迭代的流，
        DataStream<Long> odd = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {

                return value % 2 !=  0;
            }
        });

        iteration.closeWith(odd);
        SingleOutputStreamOperator<Long> res = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 0;
            }
        });

        res.print();

//        SplitStream<Long> longSplitStream = minusOne.split(new OutputSelector<Long>() {
//            @Override
//            public Iterable<String> select(Long value) {
//                List<String> output = new ArrayList<String>();
//                if (value % 2 ==  0) {
//                    output.add("even");
//                } else {
//                    output.add("odd");
//                }
//                return output;
//            }
//        });
//        // 对需要迭代的流形成一个闭环
//        iteration.closeWith(longSplitStream.select("odd"));
//
//        longSplitStream.select("even").writeAsText("/Users/mdh/Desktop/2");

        // execute the program
        env.execute("Iterative Pi Example");
    }
}
