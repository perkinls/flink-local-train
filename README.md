# Flink从入门到项目实践



> Apache Flink是一个面向分布式数据流处理和批量数据处理的开源计算平台，提供支持流处理和批处理两种类型应用的功能。文章会对Flink中基本API如：DataSet、DataStream、Table、Sql和常用特性如：Time&Window、窗口函数、Watermark、触发器、分布式缓存、异步IO、侧输出、广播和高级应用如：ProcessFunction、状态管理等知识点进行整理。
>
> 代码涵盖Java和Scala版本（因笔者时间和能力有限，代码仅供参考，如有错误的地方请多多指证）。好手不敌双拳，双拳不如四手！希望和大家一起成长、共同进步！

## 1、基本API

![img](/src/main/resources/pic/16acf2d994f3cb2e.png)

以上为Flink的运行模型（和Spark基本一致）486074-20190710082602165-1980494462.png，Flink的程序主要由三部分构成，分别为Source、Transformation、Sink。DataSource主要负责数据的读取，Transformation主要负责对属于的转换操作，Sink负责最终数据的输出。

### DataSet API

DataSet API， 对静态数据进行批处理操作，将静态数据抽象成分布式的数据集，用户可以方便地使用Flink提供的各种操作符对分布式数据集进行处理。Flink先将接入数据（如可以通过读取文件或从本地集合）来创建转换成DataSet数据集，并行分布在集群的每个节点上；然后将DataSet数据集进行各种转换操作(map，filter，union，group等)，最后通过DataSink操作将结果数据集输出到外部系统。

Flink中每一个的DataSet程序大致包含以下流程：

- ```
  - step 1 : 获得一个执行环境（ExecutionEnvironment）
  - step 2 : 加载/创建初始数据 （Source）
  - step 3 : 指定转换算子操作数据（Transformation）
  - step 4 : 指定存放结果位置（Sink）
  ```

代码案例：[Java](/src/main/java/com/lp/java/demo/dataset)  [Scala](/src/main/scala/com/lp/scala/demo/dataset)

### DataStream API

DataStream API，是Flink API中最核心的数据结构，对数据流进行流处理操作，将流式的数据抽象成分布式的数据流，用户可以方便地对分布式数据流进行各种操作。Flink先将流式数据（如可以通过消息队列，套接字流，文件等）来创建DataStream，并行分布在集群的每个节点上；然后对DataStream数据流进行转换（filter,join, update state, windows, aggregat等），最后通过DataSink操作将DataStream输出到外部文件或存储系统中。

Flink中每一个DataStream程序大致包含以下流程：

- ```
  - step 1 : 获得一个执行环境（StreamExecutionEnvironment）
  - step 2 : 加载/创建初始数据 （Source）
  - step 3 : 指定转换算子操作数据（Transformation）
  - step 4 : 指定存放结果位置（Sink）
  - step 5 : 手动触发执行
  ```

> 注意：
>
> 因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行。
>
> 在DataSet和DataStrean中transformation 都是懒执行，需要最后使用env.execute()触发执行或者使用 print(),count(),collect() 触发执行。

代码案例：[Java](/src/main/java/com/lp/java/demo/datastream)  [Scala](/src/main/scala/com/lp/scala/demo/datastream)

### Table & SQL API

Apache Flink 具有两个关系型API：Table API 和SQL。

Table & SQL API 还有另一个职责，就是流处理和批处理统一的 API 层。Flink 在 runtime 层是统一的，因为 Flink 将批任务看做流的一种特例来执行，这也是 Flink 向外鼓吹的一点。然而在编程模型上，Flink 却为批和流提供了两套 API （DataSet 和 DataStream）。为什么 runtime 统一，而编程模型不统一呢？ 在我看来，这是本末倒置的事情。用户才不管你 runtime 层是否统一，用户更关心的是写一套代码。所以 Table & SQL API 就扛起了统一API的大旗，批上的查询会随着输入数据的结束而结束并生成有限结果集，流上的查询会一直运行并生成结果流。Table & SQL API 做到了批与流上的查询具有同样的语法，因此不用改代码就能同时在批和流上跑。
Flink中每一个Table & Sql程序大致包含以下流程：

- step 1 : 获得一个执行环境（ExecutionEnvironment/StreamExecutionEnvironment）
- step 2 : 根据执行环境获取Table & Sql运行环境（TableEnvironment）
- step 3 : 注册输入表（Input table）
- step 4 : 执行Table & Sql查询
- step 5 : 输出表（Output table）结果发送到外部系统

代码案例：[Java](/src/main/java/com/lp/java/demo/datastream)  [Scala](/src/main/scala/com/lp/scala/demo/datastream)

## 2、常用特性

### 计数器



### 分布式缓存



### DataStream Kafka Source



### 自定义反列化器



### Event Time与WaterMark



### 触发器Trigger



### 侧输出

- 乱序
- 分流

### 异步IO



### 不同数据流join



### DataStream Sink



## 3、高级应用

### ProcessFunction



### 状态管理



## 4、项目案例



