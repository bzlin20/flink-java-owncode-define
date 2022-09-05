package com.ds.flink.core.mapFunction;

import com.alibaba.fastjson.JSONObject;
import com.ds.flink.core.model.fplOverview;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @ClassName DimSync
 * @Description  实时查询维表
 *  实时查询维表是指用户在 Flink 算子中直接访问外部数据库，比如用 MySQL 来进行关联，这种方式是同步方式，
 *  数据保证是最新的。但是，当我们的流计算数据过大，会对外 部系统带来巨大的访问压力，一旦出现比如连接失败、线程池满等情况，由于我们是同步调用，
 *  所以一般会导致线程阻塞、Task 等待数据返回，影响整体任务的吞吐量。而且这种方案对外部系统的 QPS 要求较高，
 *  在大数据实时计算场景下，QPS 远远高于普通的后台系统，峰值高达十万到几十万，整体作业瓶颈转移到外部系统
 * @Author ds-longju
 * @Date 2022/7/19 7:14 下午
 * @Version 1.0
 **/
public class DimSync  extends RichMapFunction<fplOverview, String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DimSync.class);
    private Connection conn = null;
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = DriverManager.getConnection("jdbc:mysql://rm-bp161be65d56kbt4nzo.mysql.rds.aliyuncs.com:3306/mysqldb?characterEncoding=UTF-8", "root", "qyllt1314#");
    }
    @Override
    public String map(fplOverview fplOverview) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(fplOverview.toJson());

        String dp_id = jsonObject.getString("dp_id");

        //根据 dp_id 查询  上周的 fpl_amount,yw
        PreparedStatement pst = conn.prepareStatement("select  max(fpl_amount) as fpl_amount,max(yearweek(datatime)) as yw \n" +
                "from fpl_overview \n" +
                "where datatime >= date_sub(curdate(), interval weekday(curdate()) + 7 Day)  # 上周第一天\n" +
                "and datatime < date_sub(curdate(), interval weekday(curdate()) + 0 Day)  # 上周最后一天+1 \n" +
                "and dp_id = ? \n" +
                "group by dp_id");
        pst.setString(1,dp_id);
        ResultSet resultSet = pst.executeQuery();
        String fpl_amount = null;
        String yw = null ;
        while (resultSet.next()){
            fpl_amount = resultSet.getString(1);
            yw = resultSet.getString(2);
        }
        String executorSql  = "select  a.fpl_amount,yearweek(a.datatime) as yw \n" +
                "from fpl_overview  a \n" +
                "inner join (\n" +
                "select dp_id,max(fpl_amount) as  fpl_amount from fpl_overview \n" +
                "where datatime >= concat(substr(date_sub(curdate(), interval 1 month),1,8),'01')   #上个月1号\n" +
                "and datatime < date_sub(curdate(), interval weekday(curdate())  Day)   # 上周\n" +
                "and  dp_id = '" + dp_id+ "' " +
                "group by  dp_id\n" +
                ") b \n" +
                "on a.dp_id = b.dp_id \n" +
                "and a.fpl_amount = b.fpl_amount \n" +
                "where datatime >= concat(substr(date_sub(curdate(), interval 1 month),1,8),'01')   #上个月1号\n" +
                "and datatime < date_sub(curdate(), interval weekday(curdate())  Day)   # 上周\n" +
                "and a.dp_id = '" + dp_id+ "' " +
                "limit 1 ";
//        System.out.println(executorSql);
        ResultSet resultSetMonth = pst.executeQuery(executorSql);
        String mon_fpl_amount = null;
        String mon_yw = null ;
        while(resultSetMonth.next()){
            mon_fpl_amount = resultSetMonth.getString(1);
            mon_yw = resultSetMonth.getString(2);
        }
        pst.close();

        jsonObject.put("lastweek_fpl_amount",fpl_amount);
        jsonObject.put("lastweek_yw",yw);
        jsonObject.put("lastmonth_fpl_amount",mon_fpl_amount);
        jsonObject.put("lastmonth_yw",mon_yw);

         return jsonObject.toString();

    }
    public void close() throws Exception {
        super.close();
        conn.close();
    }

}
