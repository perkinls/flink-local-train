package com.lp.java.demo.datastream.join;

import com.lp.java.demo.commons.BaseStreamingEnv;
import com.lp.java.demo.commons.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.processfunction.util.Split2KV;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * <p/>
 * <li>title: 双流会话窗口join</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2020/1/15 1:08 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: 会话join</li>
 * 问题：在本地做了一个实验，双流在进行join的时候，生产者在一直发送数据所有数据,所有数据都位于一个会话中，为什么进行reduce操作终端会一直输出reduce计算后的结果？
 * 未自定义触发器，不是应该在当前会话窗口结束最大的时间戳去触发计算后才输出结果吗？
 */
public class SessionWindowJoin extends BaseStreamingEnv<String> implements IBaseRunApp {

    @Override
    public void doMain() throws Exception {
        FlinkKafkaConsumer<String> consumerKv1 =
                getKafkaConsumer(KafkaConfigPo.kvTopic1, new SimpleStringSchema());
        FlinkKafkaConsumer<String> consumerKv2 =
                getKafkaConsumer(KafkaConfigPo.kvTopic2, new SimpleStringSchema());


        SingleOutputStreamOperator<Tuple2<String, Long>> operator1 = env
                .addSource(consumerKv1)
                .map(new Split2KV());

        SingleOutputStreamOperator<Tuple2<String, Long>> operator2 = env
                .addSource(consumerKv2)
                .map(new Split2KV());


        operator1
                .join(operator2)
                .where((KeySelector<Tuple2<String, Long>, String>) value -> value.f0)
                .equalTo((KeySelector<Tuple2<String, Long>, String>) value -> value.f0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))) // 会话窗口
                //使用执行的用户函数完成连接操作
                .apply((JoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple3<String, Long, Long>>) (first, second) -> {
                    Tuple3<String, Long, Long> tuple3 = new Tuple3<>();
                    tuple3.setField(first.f0, 0);
                    tuple3.setField(first.f1, 1);
                    tuple3.setField(second.f1, 2);
                    return tuple3;
                }).keyBy((KeySelector<Tuple3<String, Long, Long>, Tuple>) key -> key)
                .reduce((ReduceFunction<Tuple3<String, Long, Long>>) (v1, v2) -> {
                    Tuple3<String, Long, Long> tuple3 = new Tuple3<>();
                    tuple3.setField(v1.f0, 0);
                    tuple3.setField(v1.f1 + v2.f1, 1);
                    tuple3.setField(v1.f2 + v2.f2, 2);
                    return tuple3;
                }).print();

        env.execute(JobConfigPo.jobNamePrefix + SessionWindowJoin.class.getName());

    }
}
