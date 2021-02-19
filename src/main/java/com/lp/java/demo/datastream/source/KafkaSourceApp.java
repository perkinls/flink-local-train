package com.lp.java.demo.datastream.source;

import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.commons.BaseStreamingEnv;
import com.lp.java.demo.commons.IBaseRunApp;
import com.lp.java.demo.datastream.trigger.CustomProcessingTimeTrigger;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * <p/>
 * <li>Description: kafka消费者测试,自定义触发器</li>
 * <li>@author: panli0226@sina.com</li>
 * <li>Date: 2019-05-07 22:31</li>
 */
public class KafkaSourceApp extends BaseStreamingEnv<String> implements IBaseRunApp {


    @Override
    public void doMain() throws Exception {
        FlinkKafkaConsumer<String> kafkaConsumer = getKafkaConsumer(KafkaConfigPo.stringTopic, new SimpleStringSchema());
        AllWindowedStream<Integer, TimeWindow> stream =
                env
                        .addSource(kafkaConsumer)
                        .map(new String2Integer())
                        .timeWindowAll(Time.seconds(20))
                        .trigger(CustomProcessingTimeTrigger.create());

        stream.sum(0).print();

        // 获取JobGraph
        Iterable<JobVertex> vertices = env.getStreamGraph().getJobGraph().getVertices();
        for (JobVertex vertex : vertices) {
            System.out.println("=====>" + vertex.getName());
            System.out.println("=====>" + vertex.getID());
        }

        env.execute(JobConfigPo.jobNamePrefix + KafkaSourceApp.class.getCanonicalName());

    }


    @Override
    public TimeCharacteristic setTimeCharacter() {
        return TimeCharacteristic.ProcessingTime;
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
