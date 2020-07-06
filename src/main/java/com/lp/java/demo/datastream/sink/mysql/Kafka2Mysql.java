package com.lp.java.demo.datastream.sink.mysql;

import com.lp.scala.demo.utils.ConfigUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import com.lp.java.demo.datastream.trigger.CustomProcessingTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Properties;

/**
 * <p/>
 * <li>title: flink Sink</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2020/1/16 1:32 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: Flink中DataStream写入Mysql</li>
 */
public class Kafka2Mysql {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 设置重启策略/5次尝试/每次尝试间隔50s
         */
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 50000));

        // 选择设置时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        scala.Tuple2<String, Properties> kafkaConfig = ConfigUtils.apply("string");
        /**
         * 从最新的offset开始消费消息
         * 设置自定义时间戳分配器和watermark发射器，也可以在后面的算子中设置
         */
        FlinkKafkaConsumerBase kafkaConsumer =
                new FlinkKafkaConsumer(kafkaConfig._1, new SimpleStringSchema(), kafkaConfig._2)
                        .setStartFromLatest();

        AllWindowedStream<Integer, TimeWindow> stream = env
                .addSource(kafkaConsumer)
                .map(new String2Integer())
                .timeWindowAll(org.apache.flink.streaming.api.windowing.time.Time.seconds(20))
                .trigger(CustomProcessingTimeTrigger.create());
        SingleOutputStreamOperator<Integer> sum = stream.sum(0);
        sum.addSink(new CustomJdbcSink());
        env.execute(Kafka2Mysql.class.getCanonicalName());
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
