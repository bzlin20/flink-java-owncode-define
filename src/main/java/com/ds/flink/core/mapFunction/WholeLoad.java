package com.ds.flink.core.mapFunction;
import com.alibaba.fastjson.JSONObject;
import com.ds.flink.core.model.fplOverview;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName WholeLoad
 * @Description 预加载全量mysql数据 使用 ScheduledExecutorService 每隔 5 分钟拉取一次维表数据
 * 这种方式适用于那些实时场景不是很高，维表数据较小的场景。
 * @Author ds-longju
 * @Date 2022/7/20 10:39 上午
 * @Version 1.0
 **/
public class WholeLoad extends RichMapFunction<fplOverview,String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WholeLoad.class);
    private static Map<String,String> cache ;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    load();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        },5,5, TimeUnit.MINUTES);
    }

    @Override
    public String map(fplOverview fplOverview) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(fplOverview.toJson());
        String dp_id = jsonObject.getString("dp_id");
        String rs = cache.get(dp_id);
        JSONObject rsObject = JSONObject.parseObject(rs);
        jsonObject.putAll(rsObject);
        return jsonObject.toString();
    }

    public   void  load() throws Exception {

        Class.forName("com.mysql.jdbc.Driver");
        Connection con = DriverManager.getConnection("jdbc:mysql://rm-bp161be65d56kbt4nzo.mysql.rds.aliyuncs.com:3306/mysqldb?characterEncoding=UTF-8", "root", "qyllt1314#");
        PreparedStatement statement = con.prepareStatement("select  dp_id,max(fpl_amount) as fpl_amount,max(yearweek(datatime)) as yw \n" +
                "from fpl_overview \n" +
                "where datatime >= date_sub(curdate(), interval weekday(curdate()) + 7 Day)  # 上周第一天\n" +
                "and datatime < date_sub(curdate(), interval weekday(curdate()) + 0 Day)  # 上周最后一天+1 \n" +
                "group by dp_id");
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            String dp_id = rs.getString("dp_id");
            String fpl_amount = rs.getString("fpl_amount");
            String yw = rs.getString("yw");
            JSONObject jsonObject = JSONObject.parseObject("{}");
            jsonObject.put("lastweek_fpl_amount",fpl_amount);
            jsonObject.put("lastweek_yw",yw);
            cache.put(dp_id,jsonObject.toString());
        }
        System.out.println("数据输出测试:"+cache.toString());
        con.close();

    }
}
