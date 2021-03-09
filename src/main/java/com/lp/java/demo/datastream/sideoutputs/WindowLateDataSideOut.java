package com.lp.java.demo.datastream.sideoutputs;

import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.datastream.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.source.serialization.JsonDeserializationSchema;
import com.lp.java.demo.datastream.watermark.generator.BoundedOutOfOrderGenerator;
import net.sf.json.JSONObject;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

/**
 * <p/>
 * <li>title: 侧输出-处理窗口延迟数据</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2020/1/12 2:52 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: </li>
 */
public class WindowLateDataSideOut extends BaseStreamingEnv<JSONObject> implements IBaseRunApp {

    @Override
    public void doMain() throws Exception {
        // 定义OutputTag
        final OutputTag<JSONObject> lateOutputTag = new OutputTag<JSONObject>("late-data") {
        };

        FlinkKafkaConsumer<JSONObject> consumer =
                getKafkaConsumer(KafkaConfigPo.jsonTopic, new JsonDeserializationSchema());

        SingleOutputStreamOperator<JSONObject> reduce = env
                .addSource(consumer)
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
                .keyBy((KeySelector<JSONObject, String>) value -> value.getString("fruit"))
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 滚动窗口，大小为10s
                .allowedLateness(Time.seconds(10)) // 允许10s延迟
                .sideOutputLateData(lateOutputTag) // 延迟数据侧输出
                .reduce((ReduceFunction<JSONObject>) (v1, v2) -> {
                    String fruit = v1.getString("fruit");
                    int number = v1.getInt("number");
                    int number1 = v2.getInt("number");
                    int result = number1 + number;
                    JSONObject json = new JSONObject();
                    json.put("fruit", fruit);
                    json.put("number", result);
                    return json;
                });
        reduce.print();
        reduce.getSideOutput(lateOutputTag).print();

        env.execute(JobConfigPo.jobNamePrefix + WindowLateDataSideOut.class.getCanonicalName());
    }

    @Override
    public Integer setDefaultParallelism() {
        return 1;
    }

    @Override
    public long setWaterMarkerInterval() {
        // >0重写开启waterMark
        return 5000;
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
