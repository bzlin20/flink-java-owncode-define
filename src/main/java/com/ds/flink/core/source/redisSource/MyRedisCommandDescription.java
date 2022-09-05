package com.ds.flink.core.source.redisSource;

/**
 * @ClassName: MyRedisCommandDescription
 * @Description: redis操作描述类
 * @author: ds-longju
 * @Date: 2022-08-16 14:55
 * @Version 1.0
 **/
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;


public class MyRedisCommandDescription implements Serializable {
    public enum MyRedisCommand {
        HGET(RedisDataType.HASH);
        private RedisDataType redisDataType;

        private MyRedisCommand(RedisDataType redisDataType) {
            this.redisDataType = redisDataType;
        }
        public RedisDataType getRedisDataType() {
            return this.redisDataType;
        }
    }


    private static final long serialVersionUID = 1L;
    private MyRedisCommand redisCommand;
    private String additionalKey;

    public MyRedisCommandDescription(MyRedisCommand redisCommand, String additionalKey) {
        Preconditions.checkNotNull(redisCommand, "Redis command type can not be null");
        this.redisCommand = redisCommand;
        this.additionalKey = additionalKey;
        if ((redisCommand.getRedisDataType() == RedisDataType.HASH || redisCommand.getRedisDataType() == RedisDataType.SORTED_SET) && additionalKey == null) {
            throw new IllegalArgumentException("Hash and Sorted Set should have additional key");
        }
    }

    public MyRedisCommandDescription(MyRedisCommand redisCommand) {
        this(redisCommand, (String)null);
    }

    public MyRedisCommand getCommand() {
        return this.redisCommand;
    }

    public String getAdditionalKey() {
        return this.additionalKey;
    }
}