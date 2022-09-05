package com.ds.flink.core.source.redisSource;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @ClassName: MyMapRedisRecordSplitter
 * @Description:
 * @author: ds-longju
 * @Date: 2022-08-16 15:14
 * @Version 1.0
 **/
public class MyMapRedisRecordSplitter implements FlatMapFunction<MyRedisRecord, Tuple2<String,Integer>> {
    @Override
    public void flatMap(MyRedisRecord myRedisRecord, Collector<Tuple2<String, Integer>> collector) throws Exception {
        assert myRedisRecord.getRedisDataType() == RedisDataType.HASH;
        Map<String,String> map = (Map<String,String>)myRedisRecord.getData();
        for(Map.Entry<String,String> e : map.entrySet()){
            collector.collect(new Tuple2<>(e.getKey(),Integer.valueOf(e.getValue())));
        }

    }
}
