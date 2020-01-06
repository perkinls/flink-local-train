package com.lp.java.demo.datastream.sideoutputs;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;

/*
    原理可以参考：
    https://zhuanlan.zhihu.com/p/54583067
 */
public class SideoutputTest {
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

        SingleOutputStreamOperator<Integer> mainStream = env.addSource(kafkaConsumer010)
                .map(new String2Integer())
                .process(new ProcessFunction<Integer, Integer>() {

                    @Override
                    public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                        // emit data to regular output
                        if(value <= 5)
                            out.collect(value);
                        if(value >5 )
                        // emit data to side output
                        ctx.output(outputTag, "sideout-" + String.valueOf(value));
                    }
                });
        mainStream.print();
        mainStream.getSideOutput(outputTag).print();

        env.execute(SideoutputTest.class.getCanonicalName());
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
