 

ProcessFunction是一个比较低级的flink的流处理算子，使用该算子可以实现更细的对flink流操作的干预：

l   Events (流中的事件)

l   State(容错，一致性，仅仅用于keyed stream)

l   Timers(事件时间和处理时间，仅仅适用于keyed stream)

 

ProcessFunction可以视为是FlatMapFunction，但是它可以获取keyed state和timers。每次有事件流入processFunction算子就会触发处理。

 

为了容错，ProcessFunction可以使用RuntimeContext访问flink内部的keyed state。

 

timer允许应用程序对处理时间和事件时间的变化作出反应。每次有事件到达都会调用函数processElement(...)，该函数有个参数，也即是Context对象，该对象可以访问元素的事件时间戳和TimerService。TimerService可用于注册为后续处理事件或者事件时间的回调。当达到计时器的特定时间时，将调用onTimer(...)方法。在该调用期间，所有状态再次限定为创建计时器的key，允许计时器操纵keyed状态。

 

注意：如果要操作keyd state和timer，则必须应用ProcessFunction：

stream.keyBy(...).process(new MyProcessFunction())

 

使用CoProcessFunction可以实现更加底层的join操作。该函数的入度为2，并且对于两个输入操作会独立的调用processElement1(…)和processElement2(…)。

 

**实现底层的join****操作典型模版就是：**

l   为一个或者两个输入创建一个状态对象。

l   根据输入的事件更新状态

l   根据从另一个流接受的元素，更新状态并且产生joined结果。

 

举个例子，在进行用户数据和用户交易数据join的时候，是需要保存用户数据的状态。如果，你希望在无数据的时候有完整且准确的join结果，你可以在watermark超过交易时间的时候使用timer触发join计算和发出join结果。

 

**例子**

 

举一个wordcount例子，而且实现如果某一个key一分钟(事件时间)没有更新，就直接输出，也即是实现类似会话窗口的功能:

\1.     ValueState内部包含了计数，key和最后修改时间。

\2.     对于每一个输入的记录，ProcessFunction都会增加计数，然后修改时间戳。

\3.     该函数会在事件时间的后续1min调度回调函数。

\4.     然后根据每次回调函数，就去检查回调实践时间戳和保存的时间戳，如果匹配就将数据发出（也即是在一分钟以内没有进一步更新）。

该例子也可以可以用会话窗口实现。

 

import org.apache.flink.api.common.state.ValueState**;**

import org.apache.flink.api.common.state.ValueStateDescriptor**;**

import org.apache.flink.api.java.tuple.Tuple2**;**

import org.apache.flink.configuration.Configuration**;**

import org.apache.flink.streaming.api.functions.ProcessFunction**;**

import org.apache.flink.streaming.api.functions.ProcessFunction.Context**;**

import org.apache.flink.streaming.api.functions.ProcessFunction.OnTimerContext**;**

import org.apache.flink.util.Collector**;**

 

 

*// the source data stream*

DataStream**<**Tuple2**<**String**,** String**>>** stream **=** **...;**

 

*// apply the process function onto a keyed stream*

DataStream**<**Tuple2**<**String**,** Long**>>** result **=** stream

​    **.**keyBy**(**0**)**

​    **.**process**(new** CountWithTimeoutFunction**());**

 

*/***

 ** The data type stored in the state*

 **/*

**public** **class** **CountWithTimestamp** **{**

 

​    **public** String key**;**

​    **public** **long** count**;**

​    **public** **long** lastModified**;**

**}**

 

*/***

 ** The implementation of the ProcessFunction that maintains the count and timeouts*

 **/*

**public** **class** **CountWithTimeoutFunction** **extends** ProcessFunction**<**Tuple2**<**String**,** String**>,** Tuple2**<**String**,** Long**>>** **{**

 

​    */** The state that is maintained by this process function \*/*

​    **private** ValueState**<**CountWithTimestamp**>** state**;**

 

​    @Override

​    **public** **void** **open****(**Configuration parameters**)** **throws** Exception **{**

​        state **=** getRuntimeContext**().**getState**(new** ValueStateDescriptor**<>(**"myState"**,** CountWithTimestamp**.**class**));**

​    **}**

 

​    @Override

​    **public** **void** **processElement****(**Tuple2**<**String**,** String**>** value**,** Context ctx**,** Collector**<**Tuple2**<**String**,** Long**>>** out**)**

​            **throws** Exception **{**

 

​        *// retrieve the current count*

​        CountWithTimestamp current **=** state**.**value**();**

​        **if** **(**current **==** **null)** **{**

​            current **=** **new** CountWithTimestamp**();**

​            current**.**key **=** value**.**f0**;**

​        **}**

 

​        *// update the state's count*

​        current**.**count**++;**

 

​        *// set the state's timestamp to the record's assigned event time timestamp*

​        current**.**lastModified **=** ctx**.**timestamp**();**

 

​        *// write the state back*

​        state**.**update**(**current**);**

 

​        *// schedule the next timer 60 seconds from the current event time*

​        ctx**.**timerService**().**registerEventTimeTimer**(**current**.**lastModified **+** 60000**);**

​    **}**

 

​    @Override

​    **public** **void** **onTimer****(****long** timestamp**,** OnTimerContext ctx**,** Collector**<**Tuple2**<**String**,** Long**>>** out**)**

​            **throws** Exception **{**

 

​        *// get the state for the key that scheduled the timer*

​        CountWithTimestamp result **=** state**.**value**();**

 

​        *// check if this is an outdated timer or the latest timer*

​        **if** **(**timestamp **==** result**.**lastModified **+** 60000**)** **{**

​            *// emit the state on timeout*

​            out**.**collect**(new** Tuple2**<**String**,** Long**>(**result**.**key**,** result**.**count**));**

​        **}**

​    **}**

**}**

**keyedProcessFunction**

 

keyedProcessFunction是ProcessFunction的扩展，可以在ontimer获取timer的key。

 

```
@Override
public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
    K key = ctx.getCurrentKey();
    // ...
}
```

 

**Timers**

两种类型(事件时间和处理时间)的timer都是由TimerService维护并且以队列的形式执行。

 

TimerService会使用key和timestamp对timer进行去重，也即是对于每一对key和timestamp仅仅会存在一个timer。如果同一个timestamp注册了多个timers，onTimer()函数仅仅会调用一次。

 

对于onTimer()和processElement()方法flink是做了同步的，所以不需要关系并发问题。

 

**容错**

Timers支持容错且是应用程序checkpoint的一部分。失败重启或者从savepoint启动应用程序，timer就会被恢复。

 

**注意**：

假如checkpointed里面的基于处理时间的timer，如果在重启之前就已经超时了，那么启动后会立即输出数据。也即是在checkpoint自动重启或者利用savepoint启动程序的时候往往会发生这种情况。

 

**注意**：

除了结合RocksDBBackend/增量checkpointTimer/使用基于堆的timer。timer总是进行异步的checkpoint操作。

提醒一下，基于此种情况数量巨大的timers往往会增加checkpoint的时间，因为timer是checkpoint一部分。下面会说一下如何进行调优。

 

**Timer****合并**

由于Flink对于一个key和时间戳对只保留一个timer，那么可以通过降低timer的时间分辨率来对timer数进行合并。

 

对于一个精度为1s的时间戳，可以将目标时间取整秒数。Timer会最多提前1s钟触发，不会比基于毫米的精度要求滞后。结果就是，一个key在1秒钟最多会有一个timer。使用方法如下：

 

 

```
long coalescedTime = ((ctx.timestamp() + timeout) / 1000) * 1000;
ctx.timerService().registerProcessingTimeTimer(coalescedTime);
```

 

由于事件时间的timer仅仅会在watermark进入的时候触发，你也可以结合下个watermark调度和合并这些timer。

 

```
long coalescedTime = ctx.timerService().currentWatermark() + 1;
ctx.timerService().registerEventTimeTimer(coalescedTime);
```

 

**停止Timer**

停止处理时间的timer

```
long timestampOfTimerToStop = ...
ctx.timerService().deleteProcessingTimeTimer(timestampOfTimerToStop);
```

 

停止事件时间的timer

```
long timestampOfTimerToStop = ...
ctx.timerService().deleteEventTimeTimer(timestampOfTimerToStop);
```

 

提示一下，假如停止的timer没有被注册，调用停止方法也没问题。

 

 

 

 