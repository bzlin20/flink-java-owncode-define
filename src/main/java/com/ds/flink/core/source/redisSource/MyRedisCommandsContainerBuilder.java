package com.ds.flink.core.source.redisSource;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisSentinelConfig;
import org.apache.flink.util.Preconditions;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

/**
 * @ClassName: MyRedisCommandsContainerBuilder
 * @Description: redis读取操作对象的创建者类：该类用来根据不同的配置生成不同的对象，这里考虑了直连redis和哨兵模式两种情况
 * @author: ds-longju
 * @Date: 2022-08-16 11:55
 * @Version 1.0
 **/


public class MyRedisCommandsContainerBuilder {
    public MyRedisCommandsContainerBuilder(){

    }

    public static MyRedisCommandsContainer build(FlinkJedisConfigBase flinkJedisConfigBase) {
        if (flinkJedisConfigBase instanceof FlinkJedisPoolConfig) {
            FlinkJedisPoolConfig flinkJedisPoolConfig = (FlinkJedisPoolConfig)flinkJedisConfigBase;
            return build(flinkJedisPoolConfig);
        } else if (flinkJedisConfigBase instanceof FlinkJedisSentinelConfig) {
            FlinkJedisSentinelConfig flinkJedisSentinelConfig = (FlinkJedisSentinelConfig)flinkJedisConfigBase;
            return build(flinkJedisSentinelConfig);
        } else {
            throw new IllegalArgumentException("Jedis configuration not found");
        }
    }

    public static MyRedisCommandsContainer build(FlinkJedisPoolConfig jedisPoolConfig) {
        Preconditions.checkNotNull(jedisPoolConfig, "Redis pool config should not be Null");
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(jedisPoolConfig.getMaxIdle());
        genericObjectPoolConfig.setMaxTotal(jedisPoolConfig.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(jedisPoolConfig.getMinIdle());
        JedisPool jedisPool = new JedisPool(genericObjectPoolConfig, jedisPoolConfig.getHost(), jedisPoolConfig.getPort(), jedisPoolConfig.getConnectionTimeout(), jedisPoolConfig.getPassword(), jedisPoolConfig.getDatabase());
        return new MyRedisContainer(jedisPool);
    }

    public static MyRedisCommandsContainer build(FlinkJedisSentinelConfig jedisSentinelConfig) {
        Preconditions.checkNotNull(jedisSentinelConfig, "Redis sentinel config should not be Null");
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(jedisSentinelConfig.getMaxIdle());
        genericObjectPoolConfig.setMaxTotal(jedisSentinelConfig.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(jedisSentinelConfig.getMinIdle());
        JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(jedisSentinelConfig.getMasterName(), jedisSentinelConfig.getSentinels(), genericObjectPoolConfig, jedisSentinelConfig.getConnectionTimeout(), jedisSentinelConfig.getSoTimeout(), jedisSentinelConfig.getPassword(), jedisSentinelConfig.getDatabase());
        return new MyRedisContainer(jedisSentinelPool);
    }

}