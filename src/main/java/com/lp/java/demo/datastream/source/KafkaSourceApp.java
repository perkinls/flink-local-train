package com.lp.java.demo.datastream.source;

import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.windows.trigger.CustomProcessingTimeTrigger;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p/>
 * <li>Description: kafka消费者测试,自定义触发器</li>
 * <li>@author: panli0226@sina.com</li>
 * <li>Date: 2019-05-07 22:31</li>
 */
public class KafkaSourceApp extends BaseStreamingEnv<String> implements IBaseRunApp {

    private final static Logger log = LoggerFactory.getLogger(KafkaSourceApp.class);

    @Override
    public Integer setDefaultParallelism() {
        return 1;
    }

    @Override
    public void doMain() throws Exception {
        // 指定kafka topic和序列化方式
        FlinkKafkaConsumer<String> kafkaConsumer =
                getKafkaConsumer(KafkaConfigPo.stringTopic, new SimpleStringSchema());

        AllWindowedStream<Integer, TimeWindow> stream =
                env
                        .addSource(kafkaConsumer)
                        .map(new String2Integer())
                        .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
//                        .trigger(ProcessingTimeTrigger.create());
                        // 自定义触发器在满足条件时会出发,窗口结束时也会触发
                        .trigger(CustomProcessingTimeTrigger.create());


        // 聚合所有窗口
        stream.sum(0).print();


        // 获取JobGraph 调用 getStreamGraph 会清除 transformations 避免和 execute并行使用
//        Iterable<JobVertex> vertices = stream.getExecutionEnvironment().getStreamGraph().getJobGraph().getVertices();
//        for (JobVertex vertex : vertices) {
//            log.info("vertex ====>《Name:》 {} 《Id:》 {}", vertex.getName(), vertex.getID());
//        }

        env.execute(JobConfigPo.jobNamePrefix + KafkaSourceApp.class.getName());


    }


    private static class String2Integer extends RichMapFunction<String, Integer> {
        private static final long serialVersionUID = 1180234853172462378L;

        @Override
        public Integer map(String event) throws Exception {

            return Integer.valueOf(event);
        }
    }

}
