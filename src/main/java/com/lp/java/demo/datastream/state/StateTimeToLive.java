package com.lp.java.demo.datastream.state;

import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.datastream.IBaseRunApp;
import com.lp.java.demo.datastream.source.serialization.JsonDeserializationSchema;
import net.sf.json.JSONObject;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * @author li.pan
 * @version 1.0.0
 * @title Flink状态有效行和清理
 * @createTime 2021年03月29日 12:36:00
 * <p>
 * 案例代码主要演示State清理配置,具体配置可以参照官网更为详细。(processing time 的 TTL)
 * 官网参考地址: https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/stream/state/state.html
 * </p>
 */
public class StateTimeToLive extends BaseStreamingEnv<JSONObject> implements IBaseRunApp {
    @Override
    public void doMain() throws Exception {

        //获取kafka配置
        FlinkKafkaConsumer<JSONObject> kafkaConsumer
                = getKafkaConsumer(KafkaConfigPo.kvTopic1, new JsonDeserializationSchema());

        env
                .addSource(kafkaConsumer)
                .keyBy((KeySelector<JSONObject, String>) value -> value.getString("fruit"))
                .flatMap(new CountFruitNum())
                .print();

    }

     class CountFruitNum extends RichFlatMapFunction<JSONObject, Tuple2<String, Long>> {

        /**
         * The ValueState handle. The first field is the count, the second field a running sum.
         */
        private transient ValueState<Tuple2<String, Long>> sum;

        @Override
        public void flatMap(JSONObject input, Collector<Tuple2<String, Long>> out) throws Exception {

            // access the state value
            Tuple2<String, Long> currentSum = sum.value();

            // add the second field of the input value
            currentSum.f1 += input.getInt("number");

            // update the state
            sum.update(currentSum);

        }

        @Override
        public void open(Configuration config) {

            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.days(7))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 仅在创建和写入时更新
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 不返回过期数据
                    .build();

            ValueStateDescriptor<Tuple2<String, Long>> descriptor =
                    new ValueStateDescriptor<>(
                            "count_fruit_num_state", // state名字,用于flink内部检索
                            TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}) // 指定TypeInformation
                            );
            descriptor.enableTimeToLive(ttlConfig);
            sum = getRuntimeContext().getState(descriptor);
        }
    }

}
