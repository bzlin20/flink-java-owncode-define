package com.ds.flink.core.until;

import com.ds.flink.core.model.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName: ClickHouseUtil
 * @Description: 对clickhouse操作
 * @author: ds-longju
 * @Date: 2022-08-19 17:43
 * @Version 1.0
 **/
public class ClickHouseUtil extends RichSinkFunction<Student> {
    private ClickHouseConnection conn = null;
    public String sql;

    public ClickHouseUtil(String sql) {
        this.sql = sql;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (conn != null)
        {
            conn.close();
        }
    }

    @Override
    public void invoke(Student student, Context context) throws Exception {
        String url = "jdbc:clickhouse://120.26.126.158:8123/default";
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser("root");
        properties.setPassword("4d78476136ce20ad22902cfa2ab380fad33e59defd2696497f20e1408a86d62f");
        properties.setSessionId("default-session-id");

        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        Map<ClickHouseQueryParam, String> additionalDBParams = new HashMap<>();

        additionalDBParams.put(ClickHouseQueryParam.SESSION_ID, "new-session-id");

        try {
            conn = dataSource.getConnection();
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.setInt(1,student.getId());
            preparedStatement.setString(2, student.getName());
            preparedStatement.setString(3, student.getPassword());
            preparedStatement.setInt(4, student.getAge());

            preparedStatement.execute();
        }
        catch (Exception e){
            e.printStackTrace();
        }


    }
}
