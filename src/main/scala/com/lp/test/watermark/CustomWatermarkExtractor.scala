package com.lp.test.watermark

import net.sf.json.JSONObject
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * <p/> 
  * <li>Description: 自定义周期性时间戳分配器和watermark发射器</li>
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-05-08 22:37</li> 
  */
class CustomWatermarkExtractor extends AssignerWithPeriodicWatermarks[JSONObject] {

  var currentTimestamp = Long.MinValue

  /**
    * waterMark生成器
    * @return
    */
  override def getCurrentWatermark: Watermark = {

    new Watermark(
      if (currentTimestamp == Long.MinValue)
        Long.MinValue
      else
        currentTimestamp - 1
    )
  }

  /**
    * 时间抽取
    *
    * @param element
    * @param previousElementTimestamp
    * @return
    */
  override def extractTimestamp(element: JSONObject, previousElementTimestamp: Long): Long = {

    this.currentTimestamp = element.getLong("time")

    currentTimestamp
  }
}
