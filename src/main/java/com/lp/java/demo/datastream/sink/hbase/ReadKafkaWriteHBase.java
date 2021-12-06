package com.lp.java.demo.datastream.sink.hbase;

import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.commons.utils.HBaseUtils;
import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.datastream.source.serialization.JsonDeserializationSchema;
import net.sf.json.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author li.pan
 * @version 1.0.0
 * @title kafka数据写入HBase
 * @createTime 2021年03月09日 22:55:00
 * <p>
 * https://blog.csdn.net/u010271601/article/details/104956245/
 * https://my.oschina.net/u/2380815/blog/4454036
 * TODO ：伪代码,未完全实现
 * </p>
 */
public class ReadKafkaWriteHBase extends BaseStreamingEnv<JSONObject> implements IBaseRunApp {

    // HBase 表名
    private final static String HBASE_TABLE_NAME = "sink_json_tale";
    // HBase 列簇
    private final static String[] COLUMN_FAMILY = new String[]{"cf1", "cf2"};

    @Override
    public void doMain() throws Exception {

        // 获取Kafka消费者
        FlinkKafkaConsumer<JSONObject> kafkaConsumer
                = getKafkaConsumer(KafkaConfigPo.jsonTopic, new JsonDeserializationSchema());


        SingleOutputStreamOperator<Put> putData = env
                .addSource(kafkaConsumer)
                .process(new ProcessFunction<JSONObject, Put>() {
                    private static final long serialVersionUID = -3336812200652331944L;

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<Put> out) throws Exception {
                        String rowKey = value.getString("fruit");
                        Put put = new Put(Bytes.toBytes(rowKey));
                        // TODO 伪代码,数据包装未编写
                        // Put是构造数据
                        out.collect(put);
                    }
                });

        putData.addSink(new HBaseSinkFunction());
        env.execute(JobConfigPo.jobNamePrefix + ReadKafkaWriteHBase.class.getName());

    }

    static class HBaseSinkFunction extends RichSinkFunction<Put> {
        private static final long serialVersionUID = 6124259009774130812L;
        private transient Connection conn = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            conn = HBaseUtils.getConnection("", 2181);
            TableName table = TableName.valueOf(HBASE_TABLE_NAME);
            Admin admin = conn.getAdmin();
            if (!admin.tableExists(table)) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(HBASE_TABLE_NAME));
                for (String cf : COLUMN_FAMILY) {
                    tableDescriptor.addFamily(new HColumnDescriptor(cf));
                }
                admin.createTable(tableDescriptor);
            }
        }

        @Override
        public void invoke(Put value, Context context) throws Exception {
            Table table = conn.getTable(TableName.valueOf(HBASE_TABLE_NAME));
            table.put(value);
        }

        @Override
        public void close() throws Exception {
            super.close();
            conn.close();
        }


    }
}
