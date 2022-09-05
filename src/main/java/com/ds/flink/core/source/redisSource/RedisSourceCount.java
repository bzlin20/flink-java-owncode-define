package com.ds.flink.core.source.redisSource;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

/**
 * @ClassName: RedisSourceCount
 * @Description: 主类
 * @author: ds-longju
 * @Date: 2022-08-16 15:17
 * @Version 1.0
 **/
public class RedisSourceCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10000L);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // 配置redis的连接方式
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("120.26.126.158").setPort(6379).setPassword("000415abc").build();

        // 创建redis数据愿
        DataStreamSource<MyRedisRecord> redisSourceDataStream = env.addSource(new RedisSource(conf, new MyRedisCommandDescription(MyRedisCommandDescription.MyRedisCommand.HGET, "flink")));

        // 数据处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = redisSourceDataStream.flatMap(new MyMapRedisRecordSplitter()).timeWindowAll(Time.milliseconds(5000)).maxBy(1);

        tuple2SingleOutputStreamOperator.print();

        env.execute("FLINK SOURCE REDIS COMPUTE ") ;



    }
}
