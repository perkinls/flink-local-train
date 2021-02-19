package com.lp.java.demo.datastream;

import com.lp.java.demo.datastream.source.DataStreamSourceApp;
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
            new DataStreamSourceApp().doMain();

        } catch (Exception e) {
            log.error("程序处理错误～!");
            e.printStackTrace();
        }

    }
}
