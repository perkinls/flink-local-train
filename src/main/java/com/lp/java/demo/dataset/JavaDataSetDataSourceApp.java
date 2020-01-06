package com.lp.java.demo.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;


/**
 * <p/> DataSet  Source </li>
 * <li>@author: li.pan</li>
 * <li>Date: 2019/12/29 15:07 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: </li>
 */
public class JavaDataSetDataSourceApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 未完全测试
        textFile(env);
        fromCollection(env);
        csvFile(env);
        readRecurisFiles(env);
        readCompressionFiles(env);

    }


    /**
     * 文件 datasource
     * @param env 执行环境
     * @throws Exception
     */
    public static void textFile(ExecutionEnvironment env) throws Exception {
        String filePath = "file:///Users/rocky/IdeaProjects/imooc-workspace/data/04/hello.txt";

        env.readTextFile(filePath).print();

        System.out.println("~~~~~~~华丽的分割线~~~~~~~~");

        filePath = "file:///Users/rocky/IdeaProjects/imooc-workspace/data/04/inputs";
        env.readTextFile(filePath).print();
    }


    /**
     * 集合 datasource
     * @param env 执行环境
     * @throws Exception
     */
    public static void fromCollection(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<Integer>();
        for (int i = 1; i <= 10; i++) {
            list.add(i);
        }
        env.fromCollection(list).print();
    }


    /**
     * csv datasource
     * @param env 执行环境
     * @throws Exception
     */
    public static void csvFile (ExecutionEnvironment env) throws Exception {
        String filePath = "";
        env.readCsvFile(filePath);
    }

    /**
     * 递归，嵌套文件 datasource
     * @param env 执行环境
     * @throws Exception
     */
    public static void readRecurisFiles (ExecutionEnvironment env) throws Exception {
        String filePath = "";
        Configuration parameters = new Configuration();
        parameters.setBoolean("recursive.file.enumeration", true);
        env.readTextFile(filePath).withParameters(parameters).print();
    }


    /**
     * 压缩文件 datasource
     * @param env 执行环境
     * @throws Exception
     */
    public static void readCompressionFiles (ExecutionEnvironment env) throws Exception {
        String filePath = "";
        env.readTextFile(filePath).print();
    }


}

