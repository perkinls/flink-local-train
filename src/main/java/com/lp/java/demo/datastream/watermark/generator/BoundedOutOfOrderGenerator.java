package com.lp.java.demo.datastream.watermark.generator;

import net.sf.json.JSONObject;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author li.pan
 * @version 1.0.0
 * @title 自定义周期性 Watermark 生成器(event时间),允许乱序
 * @createTime 2021年02月25日 13:53:00
 * <p>
 * 该watermark生成器可以覆盖的场景是：
 * 数据源在一定程度上乱序(t的元素将在最早到达的时间戳为t的元素之后最多n毫秒到达)
 * </p>
 */
public class BoundedOutOfOrderGenerator implements WatermarkGenerator<JSONObject> {
    private final static Logger log = LoggerFactory.getLogger(BoundedOutOfOrderGenerator.class);

    private final long maxOutOfOrderness = 3500; // 3.5 秒

    private long currentMaxTimestamp;

    /**
     * 每来一条事件数据调用一次，可以检查或者记录事件的时间戳，或者也可以基于事件数据本身去生成 watermark。
     *
     * @param event          事件
     * @param eventTimestamp 事件时间timeStamp
     * @param output         WatermarkOutput
     */
    @Override
    public void onEvent(JSONObject event, long eventTimestamp, WatermarkOutput output) {

        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
        // log.info("当前 currentMaxTimestamp: {} ", currentMaxTimestamp);
    }

    /**
     * 周期性的调用（也许会生成新的watermark,也许不会)
     * <p>调用此方法生成 watermark 的间隔时间由 { ExecutionConfig#getAutoWatermarkInterval()} 决定。
     *
     * @param output WatermarkOutput
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // 发出的 watermark = 当前最大时间戳 - 最大乱序时间
        log.info("当前 Watermark: {} ", new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
        output.emitWatermark(
                new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1)
        );
    }




}