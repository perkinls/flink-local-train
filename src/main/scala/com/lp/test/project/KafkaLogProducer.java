package com.lp.test.project;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 *
 * <p/>
 * <li>Description: kafka生产者随机产生数据</li>
 * <li>@author: lipan@cechealth.cn</li>
 * <li>Date: 2019-04-28 20:21</li>
 */
public class KafkaLogProducer {

    public static void main(String[] args) throws InterruptedException {

        String topic = "project_test";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "master:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //创建kafka生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        while (true) {
            StringBuilder builder = new StringBuilder();

            //厂商 地区 level 时间 ip 域名 流量
            builder.append("xx").append("\t")
                    .append("CN").append("\t")
                    .append(getLevels()).append("\t")
                    .append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).append("\t")
                    .append(getIps()).append("\t")
                    .append(getDomains()).append("\t")
                    .append(getTraffic()).append("\t");
            System.out.println(builder.toString());
            producer.send(new ProducerRecord<String, String>(topic, builder.toString()));

            Thread.sleep(1000);
        }

    }

    /**
     * 流量
     *
     * @return
     */
    private static int getTraffic() {

        return new Random().nextInt(10000);
    }

    /**
     * 获取域名
     *
     * @return
     */
    private static String getDomains() {
        String[] domains = new String[]{
                "v1.go2yd.com",
                "v2.go2yd.com",
                "v3.go2yd.com",
                "v4.go2yd.com",
                "v5.go2yd.com"
        };

        return domains[new Random().nextInt(domains.length)];
    }

    /**
     * 随机获取一个ip
     *
     * @return
     */
    private static String getIps() {
        String[] ips = new String[]{
                "223.104.18.110",
                "113.101.75.194",
                "27.17.127.135",
                "183.225.139.16",
                "112.1.66.34",
                "175.148.211.190",
                "183.227.58.21"};
        return ips[new Random().nextInt(ips.length)];
    }

    /**
     * 生成level数据
     *
     * @return
     */
    public static String getLevels() {
        String[] levels = new String[]{"M", "E"};
        return levels[new Random().nextInt(levels.length)];
    }
}
