package com.lp.java.demo.commons.po;

import java.io.Serializable;

/**
 * @author li.pan
 * @version 1.0.0
 * @title 传感器实体类
 * @createTime 2021年03月08日 17:45:00
 */
public class SensorPo implements Serializable {

    private static final long serialVersionUID = 8021325306282861073L;
    /**
     * 传感器ID
     */
    private String id;
    /**
     * 时间戳
     */
    private Long timestamp;
    /**
     * 传感器温度
     */
    private Double temperature;

    public SensorPo(String id, Long timestamp, Double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return id + " " + timestamp + " " + temperature;
    }
}
