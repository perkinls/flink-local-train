package com.lp.java.demo.datastream.source.serialization;

import net.sf.json.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @author li.pan
 * @version 1.0.0
 * @Description 自定义Json反序列化
 * @createTime 2021年02月22日 19:32:00
 */
public class JsonDeserializationSchema implements SerializationSchema<JSONObject>, DeserializationSchema<JSONObject> {

    private static final long serialVersionUID = -2093569796162875267L;

    @Override
    public JSONObject deserialize(byte[] message) throws IOException {
        return JSONObject.fromObject(new String(message));
    }

    @Override
    public boolean isEndOfStream(JSONObject nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(JSONObject element) {
        return element.toString().getBytes();
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return TypeInformation.of(JSONObject.class);
    }
}
