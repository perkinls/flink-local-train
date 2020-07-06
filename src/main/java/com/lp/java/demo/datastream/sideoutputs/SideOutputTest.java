package com.lp.java.demo.datastream.sideoutputs;

import com.lp.scala.demo.utils.ConfigUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;


/**
 * <p/>
 * <li>title: 侧输出-分流</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2020/1/12 2:52 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: https://zhuanlan.zhihu.com/p/54583067
 * </li>
 */
public class SideOutputTest {
    final static OutputTag<String> outputTag = new OutputTag<String>("side-output>5") {
    };

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        /**
         * 设置重启策略/5次尝试/每次尝试间隔50s
         */
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 50000));

        // 选择设置时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        scala.Tuple2<String, Properties> kafkaConfig = ConfigUtils.apply("string");
        /**
         * 从最新的offset开始消费消息
         * 设置自定义时间戳分配器和watermark发射器，也可以在后面的算子中设置
         */
        FlinkKafkaConsumerBase kafkaConsumer =
                new FlinkKafkaConsumer(kafkaConfig._1, new SimpleStringSchema(), kafkaConfig._2)
                        .setStartFromLatest();
        SingleOutputStreamOperator mainStream = env.addSource(kafkaConsumer)
                .map(new String2Integer())
                .process(new ProcessFunction<Integer, Integer>() {

                    @Override
                    public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                        // emit data to regular output
                        if (value <= 5)
                            out.collect(value);
                        if (value > 5)
                            // emit data to side output
                            ctx.output(outputTag, "sideout-" + String.valueOf(value));
                    }
                });
        mainStream.print();
        mainStream.getSideOutput(outputTag).print();

        env.execute(SideOutputTest.class.getCanonicalName());
    }

    private static class String2Integer extends RichMapFunction<String, Integer> {
        private static final long serialVersionUID = 1180234853172462378L;

        @Override
        public Integer map(String event) throws Exception {

            return Integer.valueOf(event);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
        }
    }
}
