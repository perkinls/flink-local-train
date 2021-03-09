package com.lp.java.demo.datastream.sideoutputs;

import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.datastream.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * <p/>
 * <li>title: 侧输出-分流</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2020/1/12 2:52 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: https://zhuanlan.zhihu.com/p/54583067
 * </li>
 */
public class SplitStreamSideOut extends BaseStreamingEnv<String> implements IBaseRunApp {

    final static OutputTag<String> outputTag = new OutputTag<String>("side-output>5") {
        private static final long serialVersionUID = -5205379002618624641L;
    };

    @Override
    public void doMain() throws Exception {
        FlinkKafkaConsumer<String> kafkaConsumer
                = getKafkaConsumer(KafkaConfigPo.stringTopic, new SimpleStringSchema());

        SingleOutputStreamOperator<Integer> mainStream = env
                .addSource(kafkaConsumer)
                .map(new RichMapFunction<String, Integer>() {
                    private static final long serialVersionUID = 4882360058296378417L;

                    @Override
                    public Integer map(String value) throws Exception {
                        return Integer.valueOf(value);
                    }
                })
                // 使用process获取所有元素,根据 条件分流
                .process(new ProcessFunction<Integer, Integer>() {
                    private static final long serialVersionUID = -4237974551767895950L;

                    @Override
                    public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                        // emit data to regular output
                        if (value <= 5)
                            out.collect(value);
                        if (value > 5)
                            // emit data to side output
                            ctx.output(outputTag, "sideout-" + value);
                    }
                });

        mainStream.print();
        mainStream.getSideOutput(outputTag).print();

        env.execute(JobConfigPo.jobNamePrefix + SplitStreamSideOut.class.getName());

    }
}
