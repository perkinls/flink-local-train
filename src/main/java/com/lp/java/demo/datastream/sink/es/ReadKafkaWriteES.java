//package com.lp.java.demo.datastream.sink.es;
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.api.common.functions.RuntimeContext;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.configuration.ConfigConstants;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.configuration.RestOptions;
//import org.apache.flink.runtime.state.StateBackend;
//import org.apache.flink.runtime.state.filesystem.FsStateBackend;
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.CheckpointConfig;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.ProcessFunction;
//import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
//import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
//import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
//import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
//import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
//import org.apache.flink.util.Collector;
//import org.apache.flink.util.ExceptionUtils;
//import org.apache.http.HttpHost;
//import org.apache.http.auth.AuthScope;
//import org.apache.http.auth.UsernamePasswordCredentials;
//import org.apache.http.impl.client.BasicCredentialsProvider;
//import org.elasticsearch.ElasticsearchParseException;
//import org.elasticsearch.action.ActionRequest;
//import org.elasticsearch.action.delete.DeleteRequest;
//import org.elasticsearch.action.index.IndexRequest;
//import org.elasticsearch.action.update.UpdateRequest;
//import org.elasticsearch.client.Requests;
//import org.elasticsearch.client.RestClient;
//import org.elasticsearch.client.RestClientBuilder;
//import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
//import org.elasticsearch.common.xcontent.XContentType;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.net.SocketTimeoutException;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Properties;
//import java.util.stream.Collectors;
//
///**
// * @author li.pan
// * @version 1.0.0
// * @Description https://wangpei.blog.csdn.net/article/details/99192593
// * @createTime 2021年01月22日 16:34:00
// */
//public class ReadKafkaWriteES {
//
//
//    private static Logger logger = LoggerFactory.getLogger(ReadKafkaWriteES.class);
//
//    public static void main(String[] args) throws Exception{
//
//        /** 解析命令行参数*/
//        ParameterTool fromArgs = ParameterTool.fromArgs(args);
//        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(fromArgs.getRequired("applicationProperties"));
//
//        // checkpoint参数
//        String checkpointDirectory = parameterTool.getRequired("checkpointDirectory");
//        long checkpointSecondInterval = parameterTool.getLong("checkpointSecondInterval");
//
//        // fromKafka参数
//        String fromKafkaBootstrapServers = parameterTool.getRequired("fromKafka.bootstrap.servers");
//        String fromKafkaGroupID = parameterTool.getRequired("fromKafka.group.id");
//        String fromKafkaAutoOffsetReset= parameterTool.getRequired("fromKafka.auto.offset.reset");
//        String fromKafkaTopic = parameterTool.getRequired("fromKafka.topic");
//
//        // toES参数
//        String toESHost = parameterTool.getRequired("toES.host");
//        String toESUsername = parameterTool.getRequired("toES.username");
//        String toESPassword = parameterTool.getRequired("toES.password");
//
//
//        /** 配置运行环境*/
//        // 设置Local Web Server
//        Configuration config = new Configuration();
//        config.setInteger(RestOptions.PORT,8081);
//        config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
//        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//        // 设置StateBackend
//        env.setStateBackend((StateBackend) new FsStateBackend(checkpointDirectory, true));
//
//        // 设置Checkpoint
//        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//        checkpointConfig.setCheckpointInterval(checkpointSecondInterval * 1000);
//        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//
//        /** 配置Kafka数据源*/
//        Properties kafkaProperties = new Properties();
//        kafkaProperties.put("bootstrap.servers",fromKafkaBootstrapServers);
//        kafkaProperties.put("group.id",fromKafkaGroupID);
//        kafkaProperties.put("auto.offset.reset",fromKafkaAutoOffsetReset);
//        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<>(fromKafkaTopic, new SimpleStringSchema(), kafkaProperties);
//        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
//        DataStream<String> source = env.addSource(kafkaConsumer).name("KafkaSource").uid("source-id");
//
//        /** 简单转换加载*/
//        // 对于每条输入数据，均调用ProcessFunction
//        // ProcessFunction可产生0条或多条数据
//        SingleOutputStreamOperator<JSONObject> etl = source.process(new ProcessFunction<String, JSONObject>() {
//            @Override
//            public void processElement(String value, ProcessFunction.Context ctx, Collector<JSONObject> out) {
//                try {
//                    JSONObject obj = JSON.parseObject(value);
//                    out.collect(obj);
//                } catch (Exception ex) {
//                    logger.error("ExceptionData: {}",value,ex);
//                }
//            }
//        });
//
//        /** 配置ES目的地*/
//        // 构造HttpHost
//        List<HttpHost> httpHosts = Arrays.stream(toESHost.split(",")).map(value -> new HttpHost(value.split(":")[0], Integer.parseInt(value.split(":")[1]))).collect(Collectors.toList());
//
//        // 构造ElasticsearchSinkBuilder
//        ElasticsearchSink.Builder<JSONObject> elasticsearchSinkBuilder = new ElasticsearchSink.Builder<>(
//                httpHosts,
//                new ElasticsearchSinkFunctionImpl());
//
//        // 1、设置每次Bulk最大Action数
//        elasticsearchSinkBuilder.setBulkFlushMaxActions(100);
//
//        // 2、添加权限认证
//        RestClientFactoryImpl restClientFactory = new RestClientFactoryImpl(toESUsername, toESPassword);
//        restClientFactory.configureRestClientBuilder(RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()])));
//        elasticsearchSinkBuilder.setRestClientFactory(restClientFactory);
//
//        // 3、添加异常处理
//        elasticsearchSinkBuilder.setFailureHandler(new ActionRequestFailureHandlerImp());
//
//        // 4、构造ElasticsearchSink
//        ElasticsearchSink<JSONObject> elasticsearchSink = elasticsearchSinkBuilder.build();
//
//        etl.addSink(elasticsearchSink);
//
//        env.execute();
//    }
//
//    /**
//     * 根据输入创建一个或多个{@link ActionRequest ActionRequests}
//     * ${@link IndexRequest IndexRequest}: 索引数据
//     * ${@link DeleteRequest DeleteRequest}: 删除数据
//     * ${@link UpdateRequest UpdateRequest}: 更新数据
//     * 实现${@link ElasticsearchSinkFunction#process(Object, RuntimeContext, RequestIndexer)}方法
//     */
//    static class ElasticsearchSinkFunctionImpl implements ElasticsearchSinkFunction<JSONObject> {
//        private IndexRequest indexRequest(JSONObject element) {
//            return Requests.indexRequest()
//                    .index(element.getString("eventType"))
//                    .type("_doc")
//                    .source(element.toJSONString(), XContentType.JSON);
//        }
//
//        @Override
//        public void process(JSONObject element, RuntimeContext ctx, RequestIndexer indexer) {
//            indexer.add(indexRequest(element));
//        }
//    }
//
//    /**
//     * 配置${@link org.elasticsearch.client.RestHighLevelClient}
//     * 添加权限认证、设置超时时间等等
//     * 实现${@link RestClientFactory#configureRestClientBuilder(RestClientBuilder)}方法
//     */
//    static class RestClientFactoryImpl implements RestClientFactory {
//
//        private String username;
//
//        private String password;
//
//        private RestClientFactoryImpl(String username, String password) {
//            this.username = username;
//            this.password = password;
//        }
//
//        @Override
//        public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
//            BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
//            basicCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,password));
//
//            restClientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(basicCredentialsProvider));
//        }
//    }
//
//    /**
//     * 自定义异常处理
//     * 实现${@link ActionRequestFailureHandler#onFailure(ActionRequest, Throwable, int, RequestIndexer)}方法
//     */
//
//    @Slf4j
//    static class ActionRequestFailureHandlerImp implements ActionRequestFailureHandler {
//
//        @Override
//        public void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) {
//
//            // 异常1: ES队列满了(Reject异常)，放回队列
//            if(ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()){
//                indexer.add(action);
//
//                // 异常2: ES超时异常(timeout异常)，放回队列
//            }else if(ExceptionUtils.findThrowable(failure, SocketTimeoutException.class).isPresent()){
//                indexer.add(action);
//
//                // 异常3: ES语法异常，丢弃数据，记录日志
//            }else if(ExceptionUtils.findThrowable(failure, ElasticsearchParseException.class).isPresent()){
//                log.error("Sink to es exception ,exceptionData: {} ,exceptionStackTrace: {}",action.toString(),org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(failure));
//
//                // 异常4: 其他异常，丢弃数据，记录日志
//            }else{
//                log.error("Sink to es exception ,exceptionData: {} ,exceptionStackTrace: {}",action.toString(),org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(failure));
//            }
//        }}
//}
//
