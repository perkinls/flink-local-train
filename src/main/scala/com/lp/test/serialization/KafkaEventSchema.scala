package com.lp.test.serialization

import net.sf.json.JSONObject
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation

/**
  * <p/> 
  * <li>Description: 自定义Json反序列化器</li>
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-05-08 22:06</li> 
  */
class KafkaEventSchema extends DeserializationSchema[JSONObject] with SerializationSchema[JSONObject]{

  override def deserialize(message: Array[Byte]): JSONObject = {
    JSONObject.fromObject(new String(message))
  }

  override def isEndOfStream(nextElement: JSONObject): Boolean = false

  override def serialize(element: JSONObject): Array[Byte] = {
    element.toString().getBytes()
  }

  override def getProducedType: TypeInformation[JSONObject] = {
    TypeInformation.of(classOf[JSONObject])
  }
}
