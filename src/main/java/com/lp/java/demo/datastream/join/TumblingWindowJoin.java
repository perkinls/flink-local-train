package com.lp.java.demo.datastream.join;

import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.richfunction.RichMapSplit2KV;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * <p/>
 * <li>title: 双流滚动窗口join</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2020/1/15 1:08 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: 滚动窗口join</li>
 */
public class TumblingWindowJoin extends BaseStreamingEnv<String> implements IBaseRunApp {

    @Override
    public void doMain() throws Exception {
        FlinkKafkaConsumer<String> consumerKv1 =
                getKafkaConsumer(KafkaConfigPo.kvTopic1, new SimpleStringSchema());
        FlinkKafkaConsumer<String> consumerKv2 =
                getKafkaConsumer(KafkaConfigPo.kvTopic2, new SimpleStringSchema());


        SingleOutputStreamOperator<Tuple2<String, Long>> operator1 = env
                .addSource(consumerKv1)
                .map(new RichMapSplit2KV());

        SingleOutputStreamOperator<Tuple2<String, Long>> operator2 = env
                .addSource(consumerKv2)
                .map(new RichMapSplit2KV());

        operator1
                .join(operator2)
                .where((KeySelector<Tuple2<String, Long>, String>) value -> value.f0)
                .equalTo((KeySelector<Tuple2<String, Long>, String>) value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // 滚动窗口
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

        env.execute(JobConfigPo.jobNamePrefix + TumblingWindowJoin.class.getName());

    }
}
