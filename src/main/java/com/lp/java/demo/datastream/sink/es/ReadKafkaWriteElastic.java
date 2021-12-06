package com.lp.java.demo.datastream.sink.es;

import com.lp.java.demo.commons.po.config.EsConfigPo;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.datastream.source.serialization.JsonDeserializationSchema;
import net.sf.json.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.ExceptionUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * @author li.pan
 * @version 1.0.0
 * @Description 读取Kafka数据写入es
 * @createTime 2021年01月22日 16:34:00
 */
public class ReadKafkaWriteElastic extends BaseStreamingEnv<JSONObject> implements IBaseRunApp {


    private static Logger log = LoggerFactory.getLogger(ReadKafkaWriteElastic.class);

    @Override
    public void doMain() throws Exception {

        FlinkKafkaConsumer<JSONObject> kafkaConsumer
                = getKafkaConsumer(KafkaConfigPo.jsonTopic, new JsonDeserializationSchema());

        DataStreamSource<JSONObject> sourceData = env.addSource(kafkaConsumer);

        sourceData
                .addSink(sinkElastic())
                .setParallelism(JobConfigPo.sinkParallelism);

        env.execute();

    }

    /**
     * 数据写入Es
     *
     * @return
     */
    private ElasticsearchSink<JSONObject> sinkElastic() throws MalformedURLException {

        // 构造 httpHost
        List<HttpHost> httpHosts = getEsAddresses(EsConfigPo.clusterServers);

        // 构造 ElasticsearchSinkBuilder
        ElasticsearchSink.Builder<JSONObject> elasticsearchSinkBuilder =
                new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunctionImpl());


        /*
         * 保证数据的完整性，添加Flink自带的重试策略
         * bulk.flush.backoff.enable 用来表示是否开启重试机制
         * bulk.flush.backoff.type 重试策略，有两种：EXPONENTIAL 指数型（表示多次重试之间的时间间隔按照指数方式进行增长）、CONSTANT 常数型（表示多次重试之间的时间间隔为固定常数）
         * bulk.flush.backoff.delay 进行重试的时间间隔
         * bulk.flush.backoff.retries 失败重试的次数
         * bulk.flush.max.actions: 批量写入时的最大写入条数
         * bulk.flush.max.size.mb: 批量写入时的最大数据量
         * bulk.flush.interval.ms: 批量写入的时间间隔，配置后则会按照该时间间隔严格执行，无视上面的两个批量写入配置
         */
        elasticsearchSinkBuilder.setBulkFlushBackoff(true);
        elasticsearchSinkBuilder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.CONSTANT);
        elasticsearchSinkBuilder.setBulkFlushBackoffDelay(30000);
        elasticsearchSinkBuilder.setBulkFlushBackoffRetries(3);
        elasticsearchSinkBuilder.setBulkFlushMaxActions(100);
        elasticsearchSinkBuilder.setBulkFlushMaxSizeMb(500);
        elasticsearchSinkBuilder.setBulkFlushInterval(5000);

        if (EsConfigPo.authEnabled) {
            // 添加权限认证
            RestClientFactoryImpl restClientFactory = new RestClientFactoryImpl(EsConfigPo.authUsername, EsConfigPo.authPassword);
            restClientFactory.configureRestClientBuilder(RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()])));
            elasticsearchSinkBuilder.setRestClientFactory(restClientFactory);
        } else {
            // 为内部创建的REST客户端上的自定义配置提供RestClientFactory
            elasticsearchSinkBuilder.setRestClientFactory(
                    restClientBuilder -> {
                        // restClientBuilder.setDefaultHeaders()
                        restClientBuilder.setMaxRetryTimeoutMillis(150000);
                    }
            );
        }


        // 添加异常处理
        elasticsearchSinkBuilder.setFailureHandler(new ActionFailureHandlerImp());

        // 构造ElasticsearchSink
        return elasticsearchSinkBuilder.build();

    }


    /**
     * 解析 es hosts
     *
     * @param hosts 主机串
     * @return addresses
     */
    public static List<HttpHost> getEsAddresses(String hosts) throws MalformedURLException {
        String[] hostList = hosts.split(",");
        List<HttpHost> addresses = new ArrayList<>();
        for (String host : hostList) {
            if (host.startsWith("http")) {
                URL url = new URL(host);
                addresses.add(new HttpHost(url.getHost(), url.getPort()));
            } else {
                String[] parts = host.split(":", 2);
                if (parts.length > 1) {
                    addresses.add(new HttpHost(parts[0], Integer.parseInt(parts[1])));
                } else {
                    throw new MalformedURLException("invalid elasticsearch hosts format");
                }
            }
        }
        return addresses;
    }

    /**
     * 根据输入创建一个或多个{@link ActionRequest ActionRequests}
     * ${@link IndexRequest IndexRequest}: 索引数据
     * ${@link DeleteRequest DeleteRequest}: 删除数据
     * ${@link UpdateRequest UpdateRequest}: 更新数据
     * 实现${@link ElasticsearchSinkFunction#process(Object, RuntimeContext, RequestIndexer)}方法
     */
    static class ElasticsearchSinkFunctionImpl implements ElasticsearchSinkFunction<JSONObject> {

        private static final long serialVersionUID = 287779249429602417L;

        private IndexRequest indexRequest(JSONObject element) {
            return Requests.indexRequest()
                    .index(element.getString("fruit"))
                    .type("_doc")
                    .source(element.toString(), XContentType.JSON);
        }

        @Override
        public void process(JSONObject element, RuntimeContext ctx, RequestIndexer indexer) {
            indexer.add(indexRequest(element));
        }
    }

    /**
     * 配置${@link org.elasticsearch.client.RestHighLevelClient}
     * 添加权限认证、设置超时时间等等
     * 实现${@link RestClientFactory#configureRestClientBuilder(RestClientBuilder)}方法
     */
    static class RestClientFactoryImpl implements RestClientFactory {

        private static final long serialVersionUID = 6186616580191643505L;
        private String username;

        private String password;

        private RestClientFactoryImpl(String username, String password) {
            this.username = username;
            this.password = password;
        }

        @Override
        public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
            BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
            basicCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

            restClientBuilder.setHttpClientConfigCallback(
                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(basicCredentialsProvider));
        }
    }

    /**
     * 异常处理
     */
    static class ActionFailureHandlerImp implements ActionRequestFailureHandler {

        private static final long serialVersionUID = 5695069211656213078L;

        private final static Logger log = LoggerFactory.getLogger(ActionFailureHandlerImp.class);

        @Override
        public void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) {

            // 异常1: ES队列满了(Reject异常)，放回队列
            if (ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
                indexer.add(action);

                // 异常2: ES超时异常(timeout异常)，放回队列
            } else if (ExceptionUtils.findThrowable(failure, SocketTimeoutException.class).isPresent()) {
                indexer.add(action);

                // 异常3: ES语法异常，丢弃数据，记录日志
            } else if (ExceptionUtils.findThrowable(failure, ElasticsearchParseException.class).isPresent()) {
                log.error("Sink to es exception ,exceptionData: {} ,exceptionStackTrace: {}",
                        action.toString(), org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(failure));

                // 异常4: 其他异常，丢弃数据，记录日志
            } else {
                log.error("Sink to es exception ,exceptionData: {} ,exceptionStackTrace: {}",
                        action.toString(), org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(failure));
            }
        }
    }

}

