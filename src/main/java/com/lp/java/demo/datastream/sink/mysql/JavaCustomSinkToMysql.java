package com.lp.java.demo.datastream.sink.mysql;

import com.lp.java.demo.commons.po.StudentPo;
import com.lp.java.demo.datastream.richfunction.SinkToMysql;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <p/>
 * <li>title: DataStream Sink</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2019/12/29 5:00 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: Sink输出到Mysql</li>
 */
public class JavaCustomSinkToMysql {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source =  env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<StudentPo> studentStream =  source.map(new MapFunction<String, StudentPo>() {
            @Override
            public StudentPo map(String value) throws Exception {

                String[] splits = value.split(",");

                StudentPo stu = new StudentPo();
                stu.setId(Integer.parseInt(splits[0]));
                stu.setName(splits[1]);
                stu.setAge(Integer.parseInt(splits[2]));

                return stu;
            }
        });

        studentStream.addSink(new SinkToMysql());

        env.execute("JavaCustomSinkToMySQL");


    }
}
