package com.ds.flink.core.source.redisSource;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;

import java.io.Serializable;

/**
 * @ClassName:  MyRedisRecord
 * @Description: 抽象redis数据:封装redis数据类型和数据对象
 * @author: ds-longju
 * @Date: 2022-08-16 11:03
 * @Version 1.0
 **/
public class MyRedisRecord implements Serializable {
    private Object data;
    private RedisDataType redisDataType;

    public MyRedisRecord(Object data, RedisDataType redisDataType) {
        this.data = data;
        this.redisDataType = redisDataType;
    }

    public Object getData() {
        return data;
    }

    public RedisDataType getRedisDataType() {
        return redisDataType;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public void setRedisDataType(RedisDataType redisDataType) {
        this.redisDataType = redisDataType;
    }
}
