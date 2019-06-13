#### Flink的异步IO

Async I/O 是阿里巴巴贡献给社区的一个呼声非常高的特性，于1.2版本引入。主要目的是为了解决与外部系统交互时网络延迟成为了系统瓶颈的问题。对于实时处理，当需要使用外部存储数据染色的时候，需要小心对待，不能让与外部系统之间的交互延迟对流处理的整个工作进度起决定性的影响。

在mapfunction等算子里访问外部存储，实际上该交互过程是同步的：比如请求a发送到数据库，那么mapfunction会一直等待响应。在很多案例中，这个等待过程是非常浪费函数时间的。与数据库异步交互，意味着单个函数实例可以并发处理很多请求，同时并发接收响应。那么，等待时间由于发送其它请求和接收其它响应，被重复使用而节省了。至少，等待时间在多个请求上被摊销。这就使得很多使用案例具有更高的吞吐量。

注意：通过增加MapFunction的到一个较大的并行度也是可以改善吞吐量的，但是这就意味着更高的资源开销：更多的MapFunction实例意味着更多的task，线程，flink内部网络连接，数据库的链接，缓存，更多内部状态开销。

##### 前提

正确的实现flink的异步IO功能，需要所连接的数据库支持异步客户端。幸运的是很多流行的数据库支持这样的客户端。

假如没有异步客户端，也可以创建多个同步客户端，放到线程池里，使用线程池来完成异步功能。当然，该种方式相对于异步客户端更低效。

##### 异步IO API

flink异步IO的API支持用户在data stream中使用异步请求客户端。API自身处理与数据流的整合，消息顺序，时间时间，容错等。

假如有目标数据库的异步客户端，使用异步IO，需要实现一下三步：

- ​	实现AsyncFunction，该函数实现了请求分发的功能。

- ​	一个callback回调，该函数取回操作的结果，然后传递给ResultFuture。

- ​	对DataStream使用异步IO操作。

```
/**

 \* An implementation of the 'AsyncFunction' that sends requests and sets the callback.

 */
class AsyncDatabaseRequest extends AsyncFunction[String, (String, String)] {

​    /** The database specific client that can issue concurrent requests with callbacks */

​    lazy val client: DatabaseClient = new DatabaseClient(host, post, credentials)

​    /** The context used for the future callbacks */

​    implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())
​    override def asyncInvoke(str: String, resultFuture: ResultFuture[(String, String)]): Unit = {
​        // issue the asynchronous request, receive a future for the result

​        val resultFutureRequested: Future[String] = client.query(str)

 

​        // set the callback to be executed once the request by the client is complete

​        // the callback simply forwards the result to the result future

​        resultFutureRequested.onSuccess {

​            case result: String => resultFuture.complete(Iterable((str, result)))
​        }
​    }
}

// create the original stream

val stream: DataStream[String] = ...

// apply the async I/O transformation

val resultStream: DataStream[(String, String)] =

​    AsyncDataStream.unorderedWait(stream, new AsyncDatabaseRequest(), 1000, TimeUnit.MILLISECONDS, 100)
```

**重要提示**：第一次调用 ResultFuture.complete的时候 ResultFuture就会完成。<u>**所有后续的complete调用都会被忽略。**</u>

下面也有两个参数需要注意一下：

1. Timeout

异步IO请求被视为失败的超时时间，超过该时间异步请求就算失败。该参数主要是为了剔除死掉或者失败的请求。

2. Capacity

该参数定义了同时最多有多少个异步请求在处理。即使异步IO的方式会导致更高的吞吐量，但是对于实时应用来说该操作也是一个瓶颈。**限制并发请求数，算子不会积压过多的未处理请求，但是一旦超过容量的显示会触发背压。**

**超时处理**

当一个异步IO请求多次超时，默认情况下会抛出一个异常，然后重启job。如果想处理超时，可以覆盖AsyncFunction#timeout方法。

**结果的顺序**

AsyncFunction发起的并发请求完成的顺序是不可预期的。为了控制结果发送的顺序，flink提供了两种模式：

1). Unordered

结果记录在异步请求结束后立刻发送。流中的数据在经过该异步IO操作后顺序就和以前不一样了。当使用处理时间作为基础时间特性的时候，该方式具有极低的延迟和极低的负载。调用方式AsyncDataStream.unorderedWait(...)

2). Ordered

该种方式流的顺序会被保留。结果记录发送的顺序和异步请求被触发的顺序一样，该顺序就是愿意流中事件的顺序。为了实现该目标，操作算子会在该结果记录之前的记录为发送之前缓存该记录。这往往会引入额外的延迟和一些Checkpoint负载，因为相比于无序模式结果记录会保存在Checkpoint状态内部较长的时间。调用方式

AsyncDataStream.orderedWait(...)

**事件时间**

当使用事件时间的时候，异步IO操作也会正确的处理watermark机制。这就意味着两种order模式的具体操作如下：

1). Unordered

watermark不会超过记录，反之亦然.意味着watermark建立了一个order边界。记录仅会在两个watermark之间无序发射。watermark之后的记录仅会在watermark发送之后发送。watermark也仅会在该watermark之前的所有记录发射完成之后发送。

这就意味着在存在watermark的情况下，无序模式引入了一些与有序模式相同的延迟和管理开销。开销的大小取决于watermark的频率。

2). Ordered

watermark的顺序就如记录的顺序一样被保存。与处理时间相比，开销没有显著变化。

请记住，注入时间 Ingestion Time是基于源处理时间自动生成的watermark事件时间的特殊情况。

##### **容错担保**

异步IO操作提供了仅一次处理的容错担保。它会将在传出的异步IO请求保存于Checkpoint，然后故障恢复的时候从Checkpoint中恢复这些请求。