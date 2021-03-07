package com.lp.java.demo.datastream;

import com.lp.java.demo.datastream.join.DoubleStreamIntervalJoin;
import com.lp.java.demo.datastream.join.SessionWindowJoin;
import com.lp.java.demo.datastream.join.SlidingWindowJoin;
import com.lp.java.demo.datastream.join.TumblingWindowJoin;
import com.lp.java.demo.datastream.watermark.CustomGeneratorWaterMark;
import com.lp.java.demo.datastream.watermark.ToolGeneratorWaterMark;
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

            // WaterMark
//            new CustomGeneratorWaterMark().doMain();
            new ToolGeneratorWaterMark().doMain();

            // window窗口双流Join
//            new DoubleStreamIntervalJoin().doMain();
//            new SessionWindowJoin().doMain();
//            new SlidingWindowJoin().doMain();
//            new TumblingWindowJoin().doMain();
        } catch (Exception e) {
            log.error("流计算程序处理错误!");
            e.printStackTrace();
        }

    }
}
