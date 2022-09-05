package com.ds.flink.core.opeator.topN;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;

/**
 * @ClassName DbUtils
 * @Description 创建数据库的连接工具类
 * @Author ds-longju
 * @Date 2022/8/3 3:03 下午
 * @Version 1.0
 **/
public class DbUtils{
        private static DruidDataSource dataSource;

        public static Connection getConnection() throws Exception {
            dataSource = new DruidDataSource();
            dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
            dataSource.setUrl("jdbc:mysql://localhost:3306/testdb");
            dataSource.setUsername("root");
            dataSource.setPassword("root");
            //设置初始化连接数，最大连接数，最小闲置数
            dataSource.setInitialSize(10);
            dataSource.setMaxActive(50);
            dataSource.setMinIdle(5);
            //返回连接
            return  dataSource.getConnection();
        }
}