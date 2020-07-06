package com.lp.java.demo.datastream.watermark;

import net.sf.json.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * <p/>
 * <li>title: 自定义Json反序列化器</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2020/01/07 22:07 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: 自定义元素个数触发器</li>
 */
public class KafkaEventSchema implements DeserializationSchema<JSONObject>, SerializationSchema<JSONObject> {

    private static final long serialVersionUID = 6154188370181669758L;

    @Override
    public byte[] serialize(JSONObject event) {
        return event.toString().getBytes();
    }

    @Override
    public JSONObject deserialize(byte[] message) throws IOException {
        return JSONObject.fromObject(new String(message));
    }

    @Override
    public boolean isEndOfStream(JSONObject nextElement) {
        return false;
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return TypeInformation.of(JSONObject.class);
    }
}