/**
 * @title process包说明
 * @author li.pan
 * @Date 2021/11/21
 * DataStreamAPI提供了一系列的Low-Level转换算子。可以访问时间戳、watermark以及注册定时事件。还可以输出特定的一些事件，例如超时事件等。
 * ProcessFunction用来构建事件驱动的应用以及实现自定义的业务逻辑(使用之前的window函数和转换算子无法实现)。
 * Flink提供了8个ProcessFunction:
 * 1.ProcessFunction
 * 2.KeyedProcessFunction {@link com.lp.java.demo.datastream.process.ProcessFunctionKeyedStream}
 * 3.CoProcessFunction {@link com.lp.java.demo.datastream.process.ProcessFunctionConnectCo}
 * 4.ProcessJoinFunction {@link com.lp.java.demo.datastream.process.ProcessFunctionJoin}
 * 5.BroadcastProcessFunction {@link com.lp.java.demo.datastream.process.ProcessFunctionNoKeyedBroadcast}
 * 6.KeyedBroadcastProcessFunction {@link com.lp.java.demo.datastream.process.ProcessFunctionKeyedBroadcast}
 * 7.ProcessWindowFunction {@link com.lp.java.demo.datastream.process.ProcessFunctionWindow}
 * 8.ProcessAllWindowFunction
 */
package com.lp.java.demo.datastream.process;
