package com.ds.flink.core.source.redisSource;

import redis.clients.jedis.Jedis;

import java.io.Serializable;
import java.util.Map;

/**
 * @ClassName:  MyRedisCommandsContainer
 * @Description:  接口类 定义redis的读取操作，目前这里只写了哈希表的get操作，可以增加更多的操作
 * @author: ds-longju
 * @Date: 2022-08-16 11:03
 * @Version 1.0
 **/
public interface MyRedisCommandsContainer extends Serializable {
    Map<String,String> hget(String key);
    void close();

}
