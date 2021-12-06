package com.lp.java.demo.commons.utils;

import com.lp.java.demo.commons.po.config.MysqlConfigPo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLUtils {
    private static final Logger log = LoggerFactory.getLogger(MySQLUtils.class);

    private static volatile Connection mysqlConn;
    static {
        // 加载配置文件中的配置项
        try {
            ConfigUtils.initLoadConfig();
        } catch (Exception e) {
            throw new RuntimeException("初始化配置文件异常！");
        }
    }


    public static Connection getConnInstance() throws SQLException, ClassNotFoundException {
        // 加载配置文件中的配置项
        if (mysqlConn == null) {
            // 线程A和线程B同时看到singleton = null，如果不为null，则直接返回singleton
            synchronized (MySQLUtils.class) {
                // 线程A或线程B获得该锁进行初始化
                if (mysqlConn == null) {
                    // 其中一个线程进入该分支，另外一个线程则不会进入该分支
                    mysqlConn = getConnection();
                }
            }
        }
        return mysqlConn;
    }


    private static Connection getConnection() throws SQLException, ClassNotFoundException {

        try {
            Class.forName(MysqlConfigPo.driver);
            return DriverManager.getConnection(MysqlConfigPo.url, MysqlConfigPo.user, MysqlConfigPo.password);
        } catch (Exception e) {
            log.error("Init Connection failed, {}", e.getMessage());
            throw e;
        }

    }

    public static void close(Connection connection, PreparedStatement pstmt) {
        if (null != pstmt) {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (null != connection) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


}
