package com.ds.flink.core.sink.sinkToHive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @ClassName: HiveWriter
 * @Description: flink 写数据到hive
 * @author: ds-longju
 * @Date: 2022-08-30 10:54
 * @Version 1.0
 **/
public class HiveWriter extends RichSinkFunction<String> {
    private Connection connection = null;
    private PreparedStatement pstmt = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        connection = DriverManager.getConnection("jdbc:hive2://172.16.10.175:8191/itg", "", "");
    }

    @Override
    public void close() throws Exception {
        if (pstmt != null) pstmt.close();
        if (connection != null) connection.close();
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        String[] fields = value.split(",");
        if (fields.length < 2) return;
        String sql = "INSERT INTO testDb.testTable SELECT ?,?";
        pstmt = connection.prepareStatement(sql);
        pstmt.setString(1, fields[0]);
        pstmt.setString(2, fields[1]);
        pstmt.executeUpdate();
    }
}
