package com.kkarch.sink;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 数据库连接工具类
 */
public class DBConnectUtil {

    private static final Logger log = LoggerFactory.getLogger(DBConnectUtil.class);

    private transient static DataSource dataSource = null;

    private transient static Properties props = new Properties();

    static {
        props.put("driverClassName", "com.mysql.cj.jdbc.Driver");
        props.put("url", "jdbc:mysql://localhost:3306/my_test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true");
        props.put("username", "root");
        props.put("password", "123456");

        try {
            dataSource = DruidDataSourceFactory.createDataSource(props);
        } catch (Exception e) {
            log.error("获取数据源失败", e);
        }
    }


    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    /**
     * 提交事务
     */
    public static void commit(Connection conn) {
        if (conn != null) {
            try {
                conn.commit();
            } catch (SQLException e) {
                log.error("提交事务失败,Connection:" + conn, e);
            } finally {
                close(conn);
            }
        }
    }

    /**
     * 事务回滚
     *
     * @param conn
     */
    public static void rollback(Connection conn) {
        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException e) {
                log.error("事务回滚失败,Connection:" + conn, e);
                e.printStackTrace();
            } finally {
                close(conn);
            }
        }
    }

    /**
     * 关闭连接
     *
     * @param conn
     */
    public static void close(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.error("关闭连接失败,Connection:" + conn, e);
                e.printStackTrace();
            }
        }
    }
}
