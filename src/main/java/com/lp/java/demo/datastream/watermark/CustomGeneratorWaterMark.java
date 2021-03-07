package com.lp.java.demo.datastream.watermark;

import com.lp.java.demo.commons.BaseStreamingEnv;
import com.lp.java.demo.commons.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.source.serialization.JsonDeserializationSchema;
import com.lp.java.demo.datastream.watermark.generator.BoundedOutOfOrderGenerator;
import net.sf.json.JSONObject;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p/>
 * <li>title: 自定义实现接口生成WaterMark</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2020/01/07 22:01 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description:
 * Flink API 时需要设置一个同时包含 TimestampAssigner 和 WatermarkGenerator 的 WatermarkStrategy。
 * WatermarkStrategy 工具类中也提供了许多常用的 watermark 策略，并且用户也可以在某些必要场景下构建自己的 watermark 策略
 * </li>
 */
public class CustomGeneratorWaterMark extends BaseStreamingEnv<JSONObject> implements IBaseRunApp {

    private final static Logger log = LoggerFactory.getLogger(CustomGeneratorWaterMark.class);

    @Override
    public void doMain() throws Exception {
        FlinkKafkaConsumer<JSONObject> kafkaConsumer =
                getKafkaConsumer(KafkaConfigPo.jsonTopic, new JsonDeserializationSchema());

        /**
         * 添加数据源/ WaterMark抽取与设置 / 滚动窗口，大小为10s / 处理乱序允许10s延迟 / 终端输出
         */
        env
                .addSource(kafkaConsumer)
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
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

    @Override
    public Integer setDefaultParallelism() {
        return 1;
    }

    /**
     * 自定义WaterMark
     * https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/event_timestamps_watermarks.html
     */
    public static class CustomWatermarkStrategy implements WatermarkStrategy<JSONObject> {

        private static final long serialVersionUID = -5817759484914451755L;


        /**
         * 根据策略实例化一个可分配时间戳的 {@link TimestampAssigner}。
         *
         * @param context
         * @return
         */
        @Override
        public TimestampAssigner<JSONObject> createTimestampAssigner(TimestampAssignerSupplier.Context context) {

            return (element, recordTimestamp) -> element.getLong("time");
        }

        /**
         * 根据策略实例化一个 watermark 生成器。
         *
         * @param context
         * @return
         */
        @Override
        public WatermarkGenerator<JSONObject> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new BoundedOutOfOrderGenerator();
        }

    }

}
