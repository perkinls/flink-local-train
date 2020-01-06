package com.lp.java.demo.datastream.eventdriver;

import net.sf.json.JSONObject;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import com.lp.java.demo.datastream.watermark.KafkaEventSchema;

import java.util.Properties;

public class EventDriverTest {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 选择设置事件事件和处理事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "mt-mdh.local:9093");
        properties.setProperty("group.id", "EventDriverTest");

        FlinkKafkaConsumer010<JSONObject> kafkaConsumer010 = new FlinkKafkaConsumer010<>("jsontest",
                new KafkaEventSchema(),
                properties);

        SingleOutputStreamOperator<JSONObject> res = env
                .addSource(kafkaConsumer010)

                // 注意 keyby的目的将相同key分配到相同的并行度
                .keyBy(new KeySelector<JSONObject, String>() {
                    //这样就可以实现利用缓存高效处理了
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getString("fruit");
                    }
                })
                .reduce(new ReduceFunction<JSONObject>() {
                    @Override
                    public JSONObject reduce(JSONObject v1, JSONObject v2) throws Exception {
                        v1.put("number",v1.getInt("number")+v2.getInt("number"));
                        return v1;
                    }
                });
        res
        .print();
        env.execute(EventDriverTest.class.getCanonicalName());
    }

}
