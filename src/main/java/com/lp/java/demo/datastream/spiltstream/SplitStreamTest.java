package com.lp.java.demo.datastream.spiltstream;

import com.lp.java.demo.datastream.sideoutputs.SideOutputTest;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SplitStreamTest {
    final static OutputTag<String> outputTag = new OutputTag<String>("side-output>5"){};
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 选择设置事件事件和处理事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "mt-mdh.local:9093");
        properties.setProperty("group.id", "side");

        FlinkKafkaConsumer010<String> kafkaConsumer010 = new FlinkKafkaConsumer010<>("side",
                new SimpleStringSchema(),
                properties);

        SplitStream<Integer> splitStreams = env.addSource(kafkaConsumer010)
                .map(new String2Integer())
                .split(new OutputSelector<Integer>() {
                    @Override
                    public Iterable<String> select(Integer value) {
                        List<String> output = new ArrayList<String>();
                        if (value % 2 == 0) {
                            output.add("even");
                        } else {
                            output.add("odd");
                        }
                        return output;
                    }
                });

        DataStream<Integer> even = splitStreams.select("even");
        DataStream<Integer> odd = splitStreams.select("odd");

        even.union(odd).print();

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
