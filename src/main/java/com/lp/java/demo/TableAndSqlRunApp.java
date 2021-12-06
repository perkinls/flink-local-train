package com.lp.java.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author li.pan
 * @title Table语法或者sql语法启动类
 * @Date 2021/11/24
 */
public class TableAndSqlRunApp {

    private final static Logger log = LoggerFactory.getLogger(TableAndSqlRunApp.class);

    public static void main(String[] args) {
        try {
            // WordCount
            // new WordCountTable().doMain();
            // new WordCountSql().doMain();
            // new StreamSQLExample().doMain();
        } catch (Exception e) {
            log.error("程序处理错误,{}", e.getMessage());
            e.printStackTrace();
        }

    }
}
