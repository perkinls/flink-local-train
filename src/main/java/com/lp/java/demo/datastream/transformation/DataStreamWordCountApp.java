package com.lp.java.demo.datastream.transformation;

import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.datastream.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * <p/>
 * <li>title: 流处理处理WordCount程序</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2019/12/29 12:43 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: 使用Java API来开发Flink的实时处理应用程序
 * wc统计的数据我们源自于socket  nc -l 9999
 * </li>
 */
public class DataStreamWordCountApp extends BaseStreamingEnv<Object> implements IBaseRunApp {

    @Override
    public void doMain() throws Exception {

        // 读取数据
        DataStreamSource<String> text = env.socketTextStream("localhost", 9999);


        // transform
        text.flatMap(new MyFlatMapFunction())   //.keyBy("word")
                .keyBy((KeySelector<WordCount, String>) wc -> wc.word)
                // window和windowAll一个可以设置并行度，一个不能设置并行度
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum("count")
                .print();


        env.execute(JobConfigPo.jobNamePrefix + DataStreamTransformApp.class.getName());
    }


    public static class MyFlatMapFunction implements FlatMapFunction<String, WordCount> {

        private static final long serialVersionUID = -8175276189585341878L;

        @Override
        public void flatMap(String value, Collector<WordCount> collector) throws Exception {
            String[] tokens = value.toLowerCase().split(",");
            for (String token : tokens) {
                if (token.length() > 0) {
                    collector.collect(new WordCount(token.trim(), 1));
                }
            }
        }
    }

    public static class WordCount {
        private String word;
        private int count;

        public WordCount() {
        }

        public WordCount(String word, int count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }

}
