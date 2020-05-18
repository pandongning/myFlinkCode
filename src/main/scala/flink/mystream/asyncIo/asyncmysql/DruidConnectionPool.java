package flink.mystream.asyncIo.asyncmysql;

import com.alibaba.druid.pool.DruidDataSourceFactory;


import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class DruidConnectionPool implements Serializable {

    private transient static DataSource dataSource = null;

    private transient static Properties props = new Properties();

    static {

        props.put("driverClassName", "com.mysql.jdbc.Driver");
        props.put("url", "jdbc:mysql://172.16.200.101:3306/bigdata?characterEncoding=UTF-8");
        props.put("username", "root");
        props.put("password", "123456");
        try {
            dataSource = DruidDataSourceFactory.createDataSource(props);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private DruidConnectionPool() {
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }


}
