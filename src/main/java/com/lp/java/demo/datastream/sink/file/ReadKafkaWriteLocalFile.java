package com.lp.java.demo.datastream.sink.file;

import com.lp.java.demo.commons.BaseStreamingEnv;
import com.lp.java.demo.commons.IBaseRunApp;
import com.lp.java.demo.commons.po.config.FileConfigPo;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.concurrent.TimeUnit;

/**
 * @author li.pan
 * @version 1.0.0
 * @title 读取kafka数据写入本地文件
 * @createTime 2021年03月08日 13:42:00
 * 在流模式下使用 FileSink 时需要启用 Checkpoint ，每次做 Checkpoint 时写入完成。
 * 如果 Checkpoint 被禁用，部分文件（part file）将永远处于 'in-progress' 或 'pending' 状态，下游系统无法安全地读取。
 */
public class ReadKafkaWriteLocalFile extends BaseStreamingEnv<String> implements IBaseRunApp {


    @Override
    public void doMain() throws Exception {
        String filePath = FileConfigPo.localFile + "flink-kafka-sink";
        FlinkKafkaConsumer<String> kafkaConsumer
                = getKafkaConsumer(KafkaConfigPo.stringTopic, new SimpleStringSchema());

        DataStreamSource<String> input = env.addSource(kafkaConsumer);


        final FileSink<String> sink = FileSink
                .forRowFormat(new Path(filePath), new SimpleStringEncoder<String>("UTF-8"))
                /**
                 * 设置桶分配政策
                 * DateTimeBucketAssigner--默认的桶分配政策，默认基于时间的分配器，每小时产生一个桶，格式如下yyyy-MM-dd--HH
                 * BasePathBucketAssigner ：将所有部分文件（part file）存储在基本路径中的分配器（单个全局桶）
                 */
                .withBucketAssigner(new DateTimeBucketAssigner<>())
                /**
                 * 设置滚动策略: CheckpointRollingPolicy、DefaultRollingPolicy
                 * OnCheckpointRollingPolicy
                 */
                .withRollingPolicy(
                        /**
                         * 滚动策略决定了写出文件的状态变化过程
                         * 1. In-progress ：当前文件正在写入中
                         * 2. Pending ：当处于 In-progress 状态的文件关闭（closed）了，就变为 Pending 状态
                         * 3. Finished ：在成功的 Checkpoint 后，Pending 状态将变为 Finished 状态
                         *
                         * 观察到的现象
                         * 1.会根据本地时间和时区，先创建桶目录
                         * 2.文件名称规则：part-<subtaskIndex>-<partFileIndex>
                         * 3.在macos中默认不显示隐藏文件，需要显示隐藏文件才能看到处于In-progress和Pending状态的文件，因为文件是按照.开头命名的
                         */
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.SECONDS.toSeconds(15)) //设置滚动间隔
                                .withInactivityInterval(TimeUnit.SECONDS.toSeconds(5)) //设置不活动时间间隔
                                .withMaxPartSize(1024 * 1024 * 1024) // part文件大小
                                .build())
                .build();

        input.sinkTo(sink);
        env.execute(JobConfigPo.jobNamePrefix + ReadKafkaWriteLocalFile.class.getName());
    }
}
