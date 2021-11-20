package com.lp.java.demo.datastream.windows.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p/>
 * <li>title: DataStream 触发器</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2019/12/29 5:00 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: 自定义元素个数触发器</li>
 * {关于触发器描述请参考：http://www.lllpan.top/article/39}
 */
public class CustomProcessingTimeTrigger extends Trigger<Object, TimeWindow> {
    private static final Logger log = LoggerFactory.getLogger(CustomProcessingTimeTrigger.class);
    private static final long serialVersionUID = 1L;

    private CustomProcessingTimeTrigger() {
    }

    private static int flag = 0;

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) {
        log.info("==============================Trigger onElement==============================");
        ctx.registerProcessingTimeTimer(window.maxTimestamp());
        // CONTINUE是代表不做输出，也即是，此时我们想要实现比如100条输出一次，
        // 而不是窗口结束再输出就可以在这里实现。
        if (flag > 9) {
            log.info("触发计算 -> flag（计数标识）= " + flag + ",onElement : " + element);
            flag = 0;
            return TriggerResult.FIRE;
        } else {
            flag++;
            log.info("onElement : {}", element);
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        log.info("==============================Trigger onEventTime==============================");

        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
        log.info("==============================Trigger onProcessingTime==============================");

        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        log.info("==============================Trigger clear==============================");

        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        log.info("==============================Trigger canMerge==============================");
        return true;
    }

    /**
     * session窗口每次会调用
     * @param window
     * @param ctx
     */
    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) {
        log.info("==============================Trigger onMerge==============================");
        // only register a timer if the time is not yet past the end of the merged window
        // this is in line with the logic in onElement(). If the time is past the end of
        // the window onElement() will fire and setting a timer here would fire the window twice.
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
            ctx.registerProcessingTimeTimer(windowMaxTimestamp);
        }
    }

    @Override
    public String toString() {
        return "ProcessingTimeTrigger()";
    }

    /**
     * 创建一个自定义触发器对象
     */
    public static CustomProcessingTimeTrigger create() {
        return new CustomProcessingTimeTrigger();
    }

}