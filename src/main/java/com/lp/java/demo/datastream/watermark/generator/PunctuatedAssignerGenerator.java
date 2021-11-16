package com.lp.java.demo.datastream.watermark.generator;

import net.sf.json.JSONObject;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @author li.pan
 * @version 1.0.0
 * @title 自定义标记 Watermark 生成器(如数据中来一条记录yy,生成一条watermark)
 * @createTime 2021年02月25日 14:02:00
 * <p>
 * 标记watermark生成器观察流事件数据并在获取到带有watermark信息的特殊事件元素时发出watermark。
 * </p>
 */
public class PunctuatedAssignerGenerator implements WatermarkGenerator<JSONObject> {

    /**
     * 每来一条事件数据调用一次，可以检查或者记录事件的时间戳，或者也可以基于事件数据本身去生成 watermark。
     *
     * @param event          事件
     * @param eventTimestamp 事件时间timeStamp
     * @param output         WatermarkOutput
     */
    @Override
    public void onEvent(JSONObject event, long eventTimestamp, WatermarkOutput output) {
        if (event.getString("xx").equals("yy")) { // 当事件带有某个指定标记时，该生成器就会发出 watermark
            output.emitWatermark(new Watermark(event.getLong("time")));
        }

        // tips: 可以针对每个事件去生成watermark。但是由于每个watermark都会在下游做一些计算，因此过多的watermark会降低程序性能。
    }

    /**
     * 周期性的调用（也许会生成新的watermark,也许不会)
     * <p>调用此方法生成 watermark 的间隔时间由 { ExecutionConfig#getAutoWatermarkInterval()} 决定。
     *
     * @param output WatermarkOutput
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // onEvent 中已经实现
    }
}
