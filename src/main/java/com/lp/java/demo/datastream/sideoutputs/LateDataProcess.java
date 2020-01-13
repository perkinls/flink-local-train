package com.lp.java.demo.datastream.sideoutputs;

import com.lp.scala.demo.utils.ConfigUtils;
import net.sf.json.JSONObject;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.OutputTag;
import com.lp.java.demo.datastream.watermark.KafkaEventSchema;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * <p/>
 * <li>title: 侧输出-延迟数据处理</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2020/1/12 2:52 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: ya</li>
 */
public class LateDataProcess {
    public static void main(String[] args) throws Exception {
        // 定义OutputTag
        final OutputTag<JSONObject> lateOutputTag = new OutputTag<JSONObject>("late-data") {
        };
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setAutoWatermarkInterval(1000); //1s

        /**
         * 设置重启策略/5次尝试/每次尝试间隔50s
         */
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 50000));

        // 选择设置时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        scala.Tuple2<String, Properties> kafkaConfig = ConfigUtils.apply("json");
        /**
         * 从最新的offset开始消费消息
         * 设置自定义时间戳分配器和watermark发射器，也可以在后面的算子中设置
         */
        FlinkKafkaConsumerBase kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1, new KafkaEventSchema(), kafkaConfig._2)
                        .setStartFromLatest()
                        .assignTimestampsAndWatermarks(new CustomWatermarkExtractor());

        SingleOutputStreamOperator<JSONObject> reduce = env
                .addSource(kafkaConsumer)
//		.assignTimestampsAndWatermarks(new CustomWatermarkExtractor());	//设置自定义时间戳分配器和watermark发射器
                .keyBy(new KeySelector<JSONObject, String>() { // 注意 keyselector的使用
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getString("fruit");
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 滚动窗口，大小为10s
                .allowedLateness(Time.seconds(10)) // 允许10s延迟
                .sideOutputLateData(lateOutputTag)
                .reduce(new ReduceFunction<JSONObject>() {
                    @Override
                    public JSONObject reduce(JSONObject v1, JSONObject v2) {
                        String fruit = v1.getString("fruit");
                        int number = v1.getInt("number");
                        int number1 = v2.getInt("number");
                        int result = number1 + number;
                        JSONObject json = new JSONObject();
                        json.put("fruit", fruit);
                        json.put("number", result);
                        return json;
                    }
                });
        reduce.print();
        //reduce.getSideOutput(lateOutputTag).print();
        env.execute(LateDataProcess.class.getCanonicalName());
    }

    private static class CustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<JSONObject> {

        private static final long serialVersionUID = -742759155861320823L;

        private long currentTimestamp = Long.MIN_VALUE;

        /**
         * 抽取watermark
         * @param event
         * @param previousElementTimestamp
         * @return
         */
        @Override
        public long extractTimestamp(JSONObject event, long previousElementTimestamp) {
            this.currentTimestamp = event.getLong("time");
            return this.currentTimestamp;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            // 发射watermark
            return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
        }
    }
}
