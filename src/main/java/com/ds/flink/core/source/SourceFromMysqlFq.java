package com.ds.flink.core.source;

import com.ds.flink.core.model.Student;
import com.ds.flink.core.model.fplOverview;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @ClassName SourceFromMysqlFq
 * @Description 取最近两个月到上周的数据作为维表
 * @Author ds-longju
 * @Date 2022/7/19 3:27 下午
 * @Version 1.0
 **/
public class SourceFromMysqlFq extends RichSourceFunction<fplOverview> {
    PreparedStatement ps;
    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接。
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "select  id,dp_id,datatime,fpl_amount,yearweek(datatime) as yw\n" +
                "from fpl_overview \n" +
                "where datatime >= DATE_SUB(now(),interval 2 month)\n" +
                "and datatime <= date_sub(curdate(),INTERVAL WEEKDAY(curdate()) + 0 DAY)";
        ps = this.connection.prepareStatement(sql);
    }


    /**
     * DataStream 调用一次 run() 方法用来获取数据
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<fplOverview> ctx) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            fplOverview fplOverview = new fplOverview(
                    resultSet.getString("id"),
                    resultSet.getString("dp_id"),
                    resultSet.getString("datatime"),
                    resultSet.getString("fpl_amount"),
                    resultSet.getString("yw")

            );
            ctx.collect(fplOverview);
//            fplOverview fplOverview = new fplOverview(
//                    resultSet.getInt("id"),
//                    resultSet.getString("name").trim(),
//                    resultSet.getString("password").trim(),
//                    resultSet.getInt("age"));
//            ctx.collect(fplOverview);
        }
    }

    @Override
    public void cancel() {

    }

    /**
     * 程序执行完毕就可以进行，关闭连接和释放资源的动作了
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) { //关闭连接和释放资源
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://rm-bp161be65d56kbt4nzo.mysql.rds.aliyuncs.com:3306/mysqldb", "root", "qyllt1314#");
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }
        return con;
    }
}
