package com.ds.flink.core.test;

import com.ds.flink.core.source.SourceFromMySQL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName main2
 * @Description 执行mysql source
 * @Author ds-longju
 * @Date 2022/7/17 9:34 下午
 * @Version 1.0
 **/
public class main2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new SourceFromMySQL()).print();

        env.execute("flink add data sourc") ;

    }
}
