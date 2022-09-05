package com.ds.flink.core.mapFunction;

import com.ds.flink.core.model.fplOverview;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName JDBCAsyncFunction
 * @Description  mysql LRU 缓存 (flink 异步Id)
 * 我们利用 Flink 的 RichAsyncFunction 读取 mysql 的数据到缓存中，我们在关联维度表时先去查询缓存，如果缓存中不存在这条数据，就利用客户端去查询 mysql，然后插入到缓存中。
 * @Author ds-longju
 * @Date 2022/7/20 2:37 下午
 * @Version 1.0
 **/
public class JDBCAsyncFunction  extends RichAsyncFunction<fplOverview, JsonObject> {
    private SQLClient client;

    @Override
    public void open(Configuration parameters) throws Exception {
        Vertx vertx = Vertx.vertx(new VertxOptions()
                .setWorkerPoolSize(10)
                .setEventLoopPoolSize(10));

        JsonObject config = new JsonObject()
                .put("url", "jdbc:mysql://rm-bp161be65d56kbt4nzo.mysql.rds.aliyuncs.com:3306/mysqldb?characterEncoding=UTF-8;useSSL=false")
                .put("driver_class", "com.mysql.cj.jdbc.Driver")
                .put("max_pool_size", 10)
                .put("user", "root")
                .put("password", "qyllt1314#");

        client = JDBCClient.createShared(vertx, config);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    @Override
    public void asyncInvoke(fplOverview fplOverview, ResultFuture<JsonObject> resultFuture) throws Exception {
        client.getConnection(
                conn -> {
            if (conn.failed()) {
                return;
            }

            final SQLConnection connection = conn.result();
            // 执行sql
            connection.query("select  max(fpl_amount) as fpl_amount,max(yearweek(datatime)) as yw \n" +
                             "from fpl_overview \n" +
                            "where datatime >= date_sub(curdate(), interval weekday(curdate()) + 7 Day)  # 上周第一天\n" +
                            "and datatime < date_sub(curdate(), interval weekday(curdate()) + 0 Day)  # 上周最后一天+1 \n" +
                             "and dp_id = '" + fplOverview.getDp_id() + " ' " +
                            "group by dp_id ", res2 -> {
                ResultSet rs = new ResultSet();
                if (res2.succeeded()) {
                    rs = res2.result();
                }else{
                    System.out.println("查询数据库出错");
                }
                List<JsonObject> stores = new ArrayList<>();
                for (JsonObject json : rs.getRows()) {
                    stores.add(json);
                }
                connection.close();
                resultFuture.complete(stores);
            });
        });

    }

}
