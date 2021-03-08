package com.lp.java.demo.datastream.watermark;

import com.lp.java.demo.commons.BaseStreamingEnv;
import com.lp.java.demo.commons.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.source.serialization.JsonDeserializationSchema;
import net.sf.json.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.Duration;


/**
 * @author li.pan
 * @version 1.0.0
 * @Description WatermarkStrategy工具生成WaterMark
 * @createTime 2021年02月24日 13:49:00
 */
public class ToolGeneratorWaterMark extends BaseStreamingEnv<JSONObject> implements IBaseRunApp {
    @Override
    public void doMain() throws Exception {
        FlinkKafkaConsumer<JSONObject> kafkaConsumer =
                getKafkaConsumer(KafkaConfigPo.jsonTopic, new JsonDeserializationSchema());

        /**
         * 添加数据源/ WaterMark抽取与设置 / 滚动窗口，大小为10s / 处理乱序允许10s延迟 / 终端输出
         */
        env
                .addSource(kafkaConsumer)
                /*
                 * {WatermarkStrategy.forMonotonousTimestamps()} 递增WaterMark,当前时间戳充当WaterMark
                 */
                //.assignTimestampsAndWatermarks(
                //        WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                //                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (element, recordTimestamp) -> element.getLong("time"))
                //)

                /*
                 * {WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))} 允许延迟时间的WaterMark
                 */
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (element, recordTimestamp) -> element.getLong("time"))
                )
                .keyBy((KeySelector<JSONObject, String>) value -> value.getString("fruit"))
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .allowedLateness(Time.seconds(3))
                .reduce((ReduceFunction<JSONObject>) (v1, v2) -> {
                    // Tips 窗口累加,如果key只有一条记录 原样输出
                    String fruit = v1.getString("fruit");
                    int number1 = v1.getInt("number");
                    int number2 = v2.getInt("number");
                    int result = number1 + number2;
                    JSONObject json = new JSONObject();
                    json.put("fruit", fruit);
                    json.put("number", result);
                    return json;

                })
                .print();

        env.execute(JobConfigPo.jobNamePrefix + CustomGeneratorWaterMark.class.getName());
    }

    @Override
    public long setWaterMarkerInterval() {
        // waterMark在父类中默认禁用,重写该方法>0启用
        return 1000;
    }
}
