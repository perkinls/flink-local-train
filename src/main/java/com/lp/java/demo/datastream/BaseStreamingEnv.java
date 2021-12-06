package com.lp.java.demo.datastream;

import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.commons.utils.ConfigUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author li.pan
 * @version 1.0.0
 * @Description 流计算基类
 * @createTime 2021年02月08日 11:50:00
 */
public class BaseStreamingEnv<T> {

    private final static Logger log = LoggerFactory.getLogger(BaseStreamingEnv.class);

    protected StreamExecutionEnvironment env = getStreamEnv();


    /**
     * 获取流计算Env环境
     *
     * @return
     */
    private StreamExecutionEnvironment getStreamEnv() {
        // 加载配置文件中的配置项
        try {
            ConfigUtils.initLoadConfig();
        } catch (Exception e) {
            throw new RuntimeException("初始化配置文件异常！");
        }

        try {
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            /*
             * 设置状态（state）相关
             * https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/ops/state/state_backends.html
             */
            switch (setCheckpointStateType()) {
                case "MEMORY_STATE":
                    MemoryStateBackend memoryStateBackend = new MemoryStateBackend();
                    env.setStateBackend(memoryStateBackend); //设置checkpoint存储方式和路径目录
                    break;
                case "FS_STATE":
                    StateBackend fsStateBackend = new FsStateBackend(JobConfigPo.checkPointPath, true);
                    env.setStateBackend(fsStateBackend); //设置checkpoint存储方式和路径目录
                case "ROCKS_STATE": // 支持增量
                    RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(JobConfigPo.checkPointPath);
                    env.setStateBackend(rocksDBStateBackend);
                default:
            }


            /*
             * 设置checkpoint相关
             */
            if (enableCheckpoint()) { // 开启checkpoint
                env.enableCheckpointing(setCheckpointInterval(), setCheckPointingMode());// 设置恰一次处理语义和checkpoint基础配置项
                env.getCheckpointConfig().setCheckpointTimeout(setCheckpointTimeout()); // CheckPoint超时时间
                env.getCheckpointConfig().setMinPauseBetweenCheckpoints(setMinPauseBetweenCheckpoints()); // 两次CheckPoint中间最小时间间隔 （是指整个任务的全部checkpoint完成）
                env.getCheckpointConfig().setMaxConcurrentCheckpoints(setMaxConcurrentCheckpoints()); // 同时允许多少个Checkpoint在做快照（是指整个任务的全部checkpoint完成）
                env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3); // 容忍多少次checkpoint失败,认为是Job失败重启应用
                env.getCheckpointConfig().enableExternalizedCheckpoints(setCheckpointClearStrategy());  // checkpoint的清除策略
            }

            /*
             * RestartStrategy重启策略，在遇到不可预知的问题时。让Job从上一次完整的Checkpoint处恢复状态，保证Job和挂之前的状态保持一致
             * FixedDelayRestartStrategy 固定延时重启策略
             */
            env.setRestartStrategy(setRestartStrategy());

            // 选择设置事件事件和处理事件
            // env.setStreamTimeCharacteristic(setTimeCharacter());
            // 在 Flink 1.12 中，默认的时间属性改变成 EventTime 了
            setEnableWaterMarker(env);

            // 设置程序并行度
            env.setParallelism(setDefaultParallelism());
            log.info("StreamExecutionEnvironment 环境初始化完成 ...");
            return env;
        } catch (Exception e) {
            throw new RuntimeException("初始化 StreamExecutionEnvironment 环境配置错误！", e);
        }

    }


    /**
     * 是否开启checkpoint
     *
     * @return
     */
    protected Boolean enableCheckpoint() {
        return JobConfigPo.enableCheckpoint;
    }

    /**
     * 设置默认checkpoint时间间隔
     *
     * @return
     */
    protected Long setCheckpointInterval() {
        return JobConfigPo.checkpointInterval;
    }

    /**
     * 设置默认checkpoint模式
     *
     * @return
     */
    protected CheckpointingMode setCheckPointingMode() {
        return CheckpointingMode.EXACTLY_ONCE;
    }

    /**
     * 设置默认checkpoint超时时间
     *
     * @return
     */
    protected Long setCheckpointTimeout() {
        return JobConfigPo.checkpointTimeOut;
    }

    /**
     * 设置默认两次CheckPoint中间最小时间间隔
     *
     * @return
     */
    protected Long setMinPauseBetweenCheckpoints() {
        return JobConfigPo.checkpointMinPauseBetween;
    }

    /**
     * 设置默认同时允许多少个Checkpoint在做快照
     *
     * @return
     */
    protected Integer setMaxConcurrentCheckpoints() {
        return JobConfigPo.checkpointCurrentCheckpoints;
    }

    /**
     * 设置默认checkpoint State类型
     *
     * @return
     */
    protected String setCheckpointStateType() {
        return JobConfigPo.checkpointStateType;
    }

    /**
     * 设置默认checkpoint类型
     *
     * @return
     */
    protected CheckpointConfig.ExternalizedCheckpointCleanup setCheckpointClearStrategy() {
        return CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;
    }

    /**
     * 设置默认重启策略
     *
     * @return
     */
    protected RestartStrategies.RestartStrategyConfiguration setRestartStrategy() {
        // 固定时间间隔重启
        return RestartStrategies.fixedDelayRestart(setRestartAttempts(), setRestartAttemptsInterval());
    }

    /**
     * 设置默认重启次数
     *
     * @return
     */
    protected Integer setRestartAttempts() {
        return JobConfigPo.checkpointRestartAttempts;
    }


    /**
     * 设置默认重启时间间隔
     *
     * @return
     */
    protected Long setRestartAttemptsInterval() {
        return JobConfigPo.checkpointRestartAttemptsInterval;
    }


    /**
     * 设置默认并行度
     *
     * @return
     */
    protected Integer setDefaultParallelism() {
        return JobConfigPo.defaultParallelism;
    }


    /**
     * 设置是否开启WaterMaker,0表示禁用,>1启用
     *
     * @param env
     */
    protected void setEnableWaterMarker(StreamExecutionEnvironment env) {
        env.getConfig().setAutoWatermarkInterval(setWaterMarkerInterval());
    }

    /**
     * 设置是否开启WaterMaker,0表示禁用,>1启用
     */
    protected long setWaterMarkerInterval() {
        return 0;
    }


    /**
     * 设置kafka消费之默认消费起始位置
     *
     * @param consumer
     */
    protected void setKafkaFromOffsets(FlinkKafkaConsumer<T> consumer) {
        // 从topic中指定的group上次消费的位置开始消费，必须配置group.id参数
        consumer.setStartFromGroupOffsets();
    }

    /**
     * 获取Kafka消费者
     *
     * @param topic       指定topic
     * @param serialModel 指定序列化方式
     * @return FlinkKafkaConsumer对象
     */
    public FlinkKafkaConsumer<T> getKafkaConsumer(String topic, DeserializationSchema<T> serialModel) {
        try {
            Properties properties = new Properties();
            if (JobConfigPo.enableCheckpoint) {
                properties.setProperty("enable.auto.commit", "false"); // 关闭kafka默认自动提交
            } else {
                properties.setProperty("enable.auto.commit", "true"); // 不开启checkpoint,启动自动提交
            }
            properties.setProperty("bootstrap.servers", KafkaConfigPo.bootstrapServers);
            properties.setProperty("group.id", KafkaConfigPo.groupId);
            properties.setProperty("key.deserializer", KafkaConfigPo.keySerializer);
            properties.setProperty("value.deserializer", KafkaConfigPo.valueSerializer);
            properties.setProperty("flink.partition-discovery.interval-millis",
                    KafkaConfigPo.partitionDiscoverMillis.toString());  //自动发现kafka的partition变化

            FlinkKafkaConsumer<T> kafkaConsumer =
                    new FlinkKafkaConsumer<T>(topic, serialModel, properties);

            if (JobConfigPo.enableCheckpoint) {
                //当 checkpoint 成功时提交 offset 到 kafka
                kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
            }

            // 设置kafka消费起始位置
            setKafkaFromOffsets(kafkaConsumer);

//            kafkaConsumer.assignTimestampsAndWatermarks();

            log.info("kafka 消费者配置完成 ...");
            return kafkaConsumer;
        } catch (Exception e) {
            throw new RuntimeException("kafka 消费者配置错误！", e);
        }
    }

}
