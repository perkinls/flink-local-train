package com.lp.java.demo.datastream.watermark;

import com.lp.scala.demo.utils.ConfigUtils;
import net.sf.json.JSONObject;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * <p/>
 * <li>title: WaterMark</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2020/01/07 22:01 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: WaterMark测试</li>
 */
public class KafkaSourceWaterMark {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 设置最少一次处理语义和恰一次处理语义
//		env.enableCheckpointing(20000,CheckpointingMode.EXACTLY_ONCE);
//		checkpoint 也可以分开设置
//		env.enableCheckpointing(20000);
//		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
//		设置checkpoint目录
//		env.setStateBackend(new FsStateBackend("/hdfs/checkpoint"));
//
//        // checkpoint的清楚策略
//        env.getCheckpointConfig()
//                .enableExternalizedCheckpoints(CheckpointConfig.
//                        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		/**
		 * 设置重启策略/5次尝试/每次尝试间隔50s
		 */
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 50000));
		// 设置生成WaterMark时间
		env.getConfig().setAutoWatermarkInterval(1000); //1s

		// 设置事件时间
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		Tuple2<String, Properties> kafkaConfig = ConfigUtils.apply("json");
		/**
		 * 构建Kafka消费者
		 * 反序列化/从最新位置开始消费/指定WaterMark
		 */
		FlinkKafkaConsumerBase kafkaConsumer =
				new FlinkKafkaConsumer(kafkaConfig._1, new KafkaEventSchema(), kafkaConfig._2)
						.setStartFromLatest()
						.assignTimestampsAndWatermarks(new CustomWatermarkExtractor());


		/**
		 * 添加数据源 / 滚动窗口，大小为10s / 允许10s延迟 / 终端输出
		 */
		env
		.addSource(kafkaConsumer)
//		.assignTimestampsAndWatermarks(new CustomWatermarkExtractor());
		.keyBy(new KeySelector<JSONObject, String>() {
			@Override
			public String getKey(JSONObject value) throws Exception {
				return value.getString("fruit");
			}
		})
		.window(TumblingEventTimeWindows.of(Time.seconds(10)))
		.allowedLateness(Time.seconds(10))
		.reduce(new ReduceFunction<JSONObject>() {
			@Override
			public JSONObject reduce(JSONObject v1, JSONObject v2) {
				String fruit = v1.getString("fruit");
				int number = v1.getInt("number");
				int number1 = v1.getInt("number");
				int result = number1 +number;
				JSONObject json = new JSONObject();
				json.put("fruit",fruit);
				json.put("number",result);
				return json;
			}
		})
		.print();

		env.execute(KafkaSourceWaterMark.class.getCanonicalName());
	}

	private static class CustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<JSONObject> {

		private static final long serialVersionUID = -742759155861320823L;

		private long currentTimestamp = Long.MIN_VALUE;

		/**
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
			return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
		}
	}
}
