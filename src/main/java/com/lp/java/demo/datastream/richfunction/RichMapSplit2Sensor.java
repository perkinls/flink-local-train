package com.lp.java.demo.datastream.richfunction;

import com.lp.java.demo.commons.po.SensorPo;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @author li.pan
 * @version 1.0.0
 * @title 字符串转化为SensorPo对象
 * @createTime 2021年03月08日 22:25:00
 */
public class RichMapSplit2Sensor extends RichMapFunction<String, SensorPo> {

    private static final long serialVersionUID = -3680385144502507808L;

    @Override
    public SensorPo map(String value) throws Exception {
        String[] split = value.split(" ");
        return new SensorPo(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
    }
}
