package com.lp.java.demo.datastream;

import com.lp.java.demo.datastream.process.ProcessFunctionKeyedStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author li.pan
 * @version 1.0.0
 * @Description Streaming启动类
 * @createTime 2021年02月19日 18:50:00
 */
public class StreamingRunApp {
    private final static Logger log = LoggerFactory.getLogger(StreamingRunApp.class);

    public static void main(String[] args) {
        try {
            // 基础算子使用
//            new DataStreamWordCountApp().doMain();
//            new DataStreamTransformApp().doMain();
//            new DataStreamSourceApp().doMain();

            // AsyncIo异步IO
//            new KafkaSourceApp().doMain();
//            new AsyncIoFlatMapJoin().doMain();
//            new AsyncIoTableJoinMysql().doMain();
//            new AsyncIoTableJoinRedis().doMain();

            // WaterMark水位线
//            new CustomGeneratorWaterMark().doMain();
//            new ToolGeneratorWaterMark().doMain();

            // Windows窗口聚合函数
//            new TumblingWindowsReduceFunction().doMain();
//            new SlidingWindowsReduceFunction().doMain();
//            new SessionWindowsAggregateFunction().doMain();
//            new ProcessFunctionTumblingWindows().doMain();
//            new ProcessFunctionWithReduceFunction().doMain();
//            new ProcessFunctionWithAggregateFunction().doMain();

            // window窗口双流Join
//            new DoubleStreamIntervalJoin().doMain();
//            new SessionWindowJoin().doMain();
//            new SlidingWindowJoin().doMain();
//            new TumblingWindowJoin().doMain();

            // process 低阶函数
            new ProcessFunctionKeyedStream().doMain();
//            new ProcessFunctionBroadcastCo().doMain();

            // Sink 输出到外部系统
//            new ReadKafkaWriteLocalFile().doMain();



        } catch (Exception e) {
            log.error("流计算程序处理错误!");
            e.printStackTrace();
        }

    }
}
