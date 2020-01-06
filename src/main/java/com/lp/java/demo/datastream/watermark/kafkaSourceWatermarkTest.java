package com.lp.java.demo.datastream.watermark;

import net.sf.json.JSONObject;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import javax.annotation.Nullable;
import java.util.Properties;

/*
	watermark测试：
	// 开启checkpoint
	// 设置自动重启

	原理可以参考：
	https://zhuanlan.zhihu.com/p/55221833
	https://zhuanlan.zhihu.com/p/59376243
 */
public class kafkaSourceWatermarkTest {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().setAutoWatermarkInterval(1000); //1s

		// 设置事件时间
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "mt-mdh.local:9093");
		properties.setProperty("group.id", "jsontest");

		FlinkKafkaConsumer010<JSONObject> kafkaConsumer010 = new FlinkKafkaConsumer010<>("jsontest",
				new KafkaEventSchema(), //自定义反序列化
				properties);
		kafkaConsumer010.setStartFromLatest(); //从最新的offset开始消费消息
		kafkaConsumer010. // 设置自定义时间戳分配器和watermark发射器，也可以在后面的算子中设置
				assignTimestampsAndWatermarks(new CustomWatermarkExtractor());

		env
		.addSource(kafkaConsumer010) // 添加数据源
//		.assignTimestampsAndWatermarks(new CustomWatermarkExtractor());	//设置自定义时间戳分配器和watermark发射器
		.keyBy(new KeySelector<JSONObject, String>() { // 注意 keyselector的使用
			@Override
			public String getKey(JSONObject value) throws Exception {
				return value.getString("fruit");
			}
		})
		.window(TumblingEventTimeWindows.of(Time.seconds(10))) // 滚动窗口，大小为10s
		.allowedLateness(Time.seconds(10)) // 允许10s延迟
		.reduce(new ReduceFunction<JSONObject>() {
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

		env.execute(kafkaSourceWatermarkTest.class.getCanonicalName());
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
