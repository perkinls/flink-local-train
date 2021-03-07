//package com.lp.java.demo.datastream.sideoutputs;
//
//import com.lp.java.demo.commons.BaseStreamingEnv;
//import com.lp.java.demo.commons.IBaseRunApp;
//import com.lp.java.demo.commons.po.config.KafkaConfigPo;
//import com.lp.java.demo.datastream.source.serialization.JsonDeserializationSchema;
//import net.sf.json.JSONObject;
//import org.apache.flink.api.common.eventtime.*;
//import org.apache.flink.api.common.functions.ReduceFunction;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.util.OutputTag;
//
//import javax.annotation.Nullable;
//
///**
// * <p/>
// * <li>title: 侧输出-延迟数据处理</li>
// * <li>@author: li.pan</li>
// * <li>Date: 2020/1/12 2:52 下午</li>
// * <li>Version: V1.0</li>
// * <li>Description: ya</li>
// */
//public class LateDataProcess extends BaseStreamingEnv<JSONObject> implements IBaseRunApp {
//    public static void main(String[] args) throws Exception {
////        // 定义OutputTag
////        final OutputTag<JSONObject> lateOutputTag = new OutputTag<JSONObject>("late-data") {
////        };
////        // set up the streaming execution environment
////        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
////
////        env.getConfig().setAutoWatermarkInterval(1000); //1s
////
////        /**
////         * 设置重启策略/5次尝试/每次尝试间隔50s
////         */
////        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 50000));
////
////        // 选择设置时间
////        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
////        env.setParallelism(1);
////
////        scala.Tuple2<String, Properties> kafkaConfig = ConfigUtils.apply("json");
////        /**
////         * 从最新的offset开始消费消息
////         * 设置自定义时间戳分配器和watermark发射器，也可以在后面的算子中设置
////         */
////        FlinkKafkaConsumerBase kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1, new KafkaEventSchema(), kafkaConfig._2)
////                .setStartFromLatest()
////                .assignTimestampsAndWatermarks(new CustomWatermarkExtractor());
////
////        SingleOutputStreamOperator<JSONObject> reduce = env
////                .addSource(kafkaConsumer)
//////		.assignTimestampsAndWatermarks(new CustomWatermarkExtractor());	//设置自定义时间戳分配器和watermark发射器
////                .keyBy(new KeySelector<JSONObject, String>() { // 注意 keyselector的使用
////                    @Override
////                    public String getKey(JSONObject value) throws Exception {
////                        return value.getString("fruit");
////                    }
////                })
////                .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 滚动窗口，大小为10s
////                .allowedLateness(Time.seconds(10)) // 允许10s延迟
////                .sideOutputLateData(lateOutputTag)
////                .reduce(new ReduceFunction<JSONObject>() {
////                    @Override
////                    public JSONObject reduce(JSONObject v1, JSONObject v2) {
////                        String fruit = v1.getString("fruit");
////                        int number = v1.getInt("number");
////                        int number1 = v2.getInt("number");
////                        int result = number1 + number;
////                        JSONObject json = new JSONObject();
////                        json.put("fruit", fruit);
////                        json.put("number", result);
////                        return json;
////                    }
////                });
////        reduce.print();
////        //reduce.getSideOutput(lateOutputTag).print();
////        env.execute(LateDataProcess.class.getCanonicalName());
//    }
//
//    @Override
//    public void doMain() throws Exception {
//        // 定义OutputTag
//        final OutputTag<JSONObject> lateOutputTag = new OutputTag<JSONObject>("late-data") {
//        };
//
//
//        FlinkKafkaConsumer<JSONObject> consumer = getKafkaConsumer(KafkaConfigPo.jsonTopic, new JsonDeserializationSchema());
//
//
//        SingleOutputStreamOperator<JSONObject> reduce = env
//                .addSource(consumer)
//                .assignTimestampsAndWatermarks()
////		.assignTimestampsAndWatermarks(new CustomWatermarkExtractor());	//设置自定义时间戳分配器和watermark发射器
//                .keyBy(new KeySelector<JSONObject, String>() { // 注意 keyselector的使用
//                    @Override
//                    public String getKey(JSONObject value) throws Exception {
//                        return value.getString("fruit");
//                    }
//                })
//                .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 滚动窗口，大小为10s
//                .allowedLateness(Time.seconds(10)) // 允许10s延迟
//                .sideOutputLateData(lateOutputTag)
//                .reduce(new ReduceFunction<JSONObject>() {
//                    @Override
//                    public JSONObject reduce(JSONObject v1, JSONObject v2) {
//                        String fruit = v1.getString("fruit");
//                        int number = v1.getInt("number");
//                        int number1 = v2.getInt("number");
//                        int result = number1 + number;
//                        JSONObject json = new JSONObject();
//                        json.put("fruit", fruit);
//                        json.put("number", result);
//                        return json;
//                    }
//                });
//        reduce.print();
//        //reduce.getSideOutput(lateOutputTag).print();
//        env.execute(LateDataProcess.class.getCanonicalName());
//
//    }
//
//    @Override
//    public Integer setDefaultParallelism() {
//        return 1;
//    }
//
//    @Override
//    public long setWaterMarkerInterval() {
//        // >0重写开启waterMark
//        return 5000;
//    }
//
//    private static class CustomWatermarkExtractor implements WatermarkStrategy<JSONObject> {
//
//        private static final long serialVersionUID = -742759155861320823L;
//
//        private long currentTimestamp = Long.MIN_VALUE;
//
//        /**
//         * 抽取watermark
//         *
//         * @param event
//         * @param previousElementTimestamp
//         * @return
//         */
//        @Override
//        public long extractTimestamp(JSONObject event, long previousElementTimestamp) {
//            this.currentTimestamp = event.getLong("time");
//            return this.currentTimestamp;
//        }
//
//        @Nullable
//        @Override
//        public Watermark getCurrentWatermark() {
//            // 发射watermark
//            return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
//        }
//    }
//
//
//    /**
//     * https://blog.csdn.net/Jerseywwwwei/article/details/108028528
//     */
//    public static class CustomWatermarkExtractor1 implements WatermarkStrategy<JSONObject> {
//
//        private static final long serialVersionUID = -5817759484914451755L;
//
//        private long currentTimestamp = Long.MIN_VALUE;
//
//
//        @Override
//        public WatermarkGenerator<JSONObject> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
//            return new WatermarkGenerator<JSONObject>() {
//                private long maxTimesStamp = Long.MIN_VALUE;
//                // 每来一条数据，将这条数据与maxTimesStamp比较，看是否需要更新watermark
//                @Override
//                public void onEvent(JSONObject event, long eventTimestamp, WatermarkOutput output) {
//                    maxTimesStamp = Math.max(event.getLong("time"), maxTimesStamp);
//                }
//
//                // 周期性更新watermark
//                @Override
//                public void onPeriodicEmit(WatermarkOutput output) {
//                    // 允许乱序数据的最大限度为3s
//                    long maxOutOfOrderness = 3000L;
//                    output.emitWatermark(new Watermark(maxTimesStamp - maxOutOfOrderness));
//                }
//            };
//        }
//
//    }
//}
