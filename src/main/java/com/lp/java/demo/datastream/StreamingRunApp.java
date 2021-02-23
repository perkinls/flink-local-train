package com.lp.java.demo.datastream;

import com.lp.java.demo.datastream.asyncio.AsyncIoTableJoinMysql;
import com.lp.java.demo.datastream.asyncio.AsyncIoTableJoinRedis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author li.pan
 * @version 1.0.0
 * @Description Streaming启动类
 * @createTime 2021年02月19日 17:50:00
 */
public class StreamingRunApp {
    private final static Logger log = LoggerFactory.getLogger(StreamingRunApp.class);

    public static void main(String[] args) {
        try {
//            new DataStreamWordCountApp().doMain();
//            new DataStreamTransformApp().doMain();
//            new DataStreamSourceApp().doMain();
//            new KafkaSourceApp().doMain();
//            new AsyncIoFlatMapJoin().doMain();
            new AsyncIoTableJoinMysql().doMain();
//            new AsyncIoTableJoinRedis().doMain();
        } catch (Exception e) {
            log.error("流计算程序处理错误!");
            e.printStackTrace();
        }

    }
}
