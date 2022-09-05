package com.ds.flink.core.opeator.topN;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;

/**
 * @ClassName MySqlSink
 * @Description 请描述类的业务用途
 * @Author ds-longju
 * @Date 2022/8/3 3:09 下午
 * @Version 1.0
 **/
public class MySqlSink extends RichSinkFunction<List<ItemBuyCount>> {
    private PreparedStatement ps;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //获取数据库连接，准备写入数据库
        connection = DbUtils.getConnection();
        String sql = "insert into itembuycount(itemId, buyCount, createDate) values (?, ?, ?); ";
        ps = connection.prepareStatement(sql);
        System.out.println("-------------open------------");
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭并释放资源
        if(connection != null) {
            connection.close();
        }

        if(ps != null) {
            ps.close();
        }
        System.out.println("-------------close------------");
    }

    @Override
    public void invoke(List<ItemBuyCount> topNItems, Context context) throws Exception {
        for(ItemBuyCount itemBuyCount : topNItems) {
            ps.setLong(1, itemBuyCount.itemId);
            ps.setLong(2, itemBuyCount.buyCount);
            ps.setTimestamp(3, new Timestamp(itemBuyCount.windowEnd));
            ps.addBatch();
        }

        //一次性写入
        int[] count = ps.executeBatch();
        System.out.println("-------------invoke------------");
        System.out.println("成功写入Mysql数量：" + count.length);

    }
}
