package com.lp.java.demo.datastream.join;

import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.datastream.IBaseRunApp;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.richfunction.RichMapSplit2KV;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * <p/>
 * <li>title: 双流间隔join</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2020/1/15 1:08 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: 间隔join</li>
 * 测试的时候将kafkaProduce中类kv数据生成方法 join情况打开
 * intervalJoin 参考文章: http://www.lllpan.top/article/42#menu_3
 */
public class DoubleStreamIntervalJoin extends BaseStreamingEnv<String> implements IBaseRunApp {

    @Override
    public void doMain() throws Exception {

        FlinkKafkaConsumer<String> consumerKv1 =
                getKafkaConsumer(KafkaConfigPo.kvTopic1, new SimpleStringSchema());
        FlinkKafkaConsumer<String> consumerKv2 =
                getKafkaConsumer(KafkaConfigPo.kvTopic2, new SimpleStringSchema());


        SingleOutputStreamOperator<Tuple2<String, Long>> operator = env
                .addSource(consumerKv1)
                .map(new RichMapSplit2KV());

        SingleOutputStreamOperator<Tuple2<String, Long>> operator1 = env
                .addSource(consumerKv2)
                .map(new RichMapSplit2KV());

        operator
                .keyBy((KeySelector<Tuple2<String, Long>, Tuple>) value -> value)
                .intervalJoin(operator1.keyBy((KeySelector<Tuple2<String, Long>, Tuple>) value -> value))
                .between(Time.milliseconds(-2), Time.milliseconds(1))
                // 执行join后函数
                .process(new ProcessJoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple2<String, Long>>() {

                    private static final long serialVersionUID = 411509804425946910L;

                    @Override
                    public void processElement(Tuple2<String, Long> left, Tuple2<String, Long> right, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        Tuple2<String, Long> tuple2 = new Tuple2<>();
                        tuple2.setField(left.f0, 0);
                        tuple2.setField(left.f1 + right.f1, 1);
                        out.collect(tuple2);
                    }
                })
                .print();

        env.execute(DoubleStreamIntervalJoin.class.getCanonicalName());

    }
}
