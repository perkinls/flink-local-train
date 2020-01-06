package com.lp.java.demo.datastream.sink.mysql;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import com.lp.java.demo.datastream.trigger.CustomProcessingTimeTrigger;

import java.util.Properties;

public class Kafka2Mysql {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 选择设置事件事件和处理事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "mt-mdh.local:9093");
        properties.setProperty("group.id", "jdbc");

        FlinkKafkaConsumer010<String> kafkaConsumer010 = new FlinkKafkaConsumer010<>("jdbc",
                new SimpleStringSchema(),
                properties);

        AllWindowedStream<Integer, TimeWindow> stream = env
                .addSource(kafkaConsumer010)
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
