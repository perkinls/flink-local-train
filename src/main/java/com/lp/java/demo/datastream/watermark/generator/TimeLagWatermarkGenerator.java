package com.lp.java.demo.datastream.watermark.generator;

import net.sf.json.JSONObject;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @author li.pan
 * @version 1.0.0
 * @title 自定义周期性 Watermark 生成器(processTime时间)
 * @createTime 2021年02月25日 13:57:00
 * <p>
 * 该生成器生成的 watermark 滞后于处理时间固定量。它假定元素会在有限延迟后到达 Flink。
 * </p>
 */
public class TimeLagWatermarkGenerator implements WatermarkGenerator<JSONObject> {
    private final long maxTimeLag = 5000; // 5 秒

    /**
     * 每来一条事件数据调用一次，可以检查或者记录事件的时间戳，或者也可以基于事件数据本身去生成 watermark。
     *
     * @param event          事件
     * @param eventTimestamp 事件时间timeStamp
     * @param output         WatermarkOutput
     */
    @Override
    public void onEvent(JSONObject event, long eventTimestamp, WatermarkOutput output) {
        // 处理时间场景下不需要实现
    }

    /**
     * 周期性的调用（也许会生成新的watermark,也许不会)
     * <p>调用此方法生成 watermark 的间隔时间由 { ExecutionConfig#getAutoWatermarkInterval()} 决定。
     *
     * @param output WatermarkOutput
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimeLag));
    }
}
