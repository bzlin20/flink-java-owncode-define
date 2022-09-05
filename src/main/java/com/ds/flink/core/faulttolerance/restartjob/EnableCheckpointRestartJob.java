package com.ds.flink.core.faulttolerance.restartjob;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * @ClassName: EnableCheckpointRestartJob
 * @Description: 演示开启Checkpoint之后，Flink作业异常后的默认重启作业策略（固定频率），最大重试次数为Integer.MAX_VALUE
 * @author: ds-longju
 * @Date: 2022-09-02 14:47
 * @Version 1.0
 **/
public class EnableCheckpointRestartJob {
    public static void main(String[] args) throws Exception {
        Logger log = LoggerFactory.getLogger(NoRestartJob.class);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //开启checkpoint
        env.enableCheckpointing(2000L);
        DataStreamSource<Tuple3<String, Integer, Long>> source = env.addSource(new SourceFunction<Tuple3<String, Integer, Long>>() {
            @Override
            public void run(SourceContext<Tuple3<String, Integer, Long>> sourceContext) throws Exception {
                int index = 1;
                while (true) {
                    sourceContext.collect(new Tuple3<>("key", index++, System.currentTimeMillis()));
                    Thread.sleep(100);
                }
            }
            @Override
            public void cancel() {

            }
        });

        source.map(new MapFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map(Tuple3<String, Integer, Long> event) throws Exception {
                if(event.f1 % 10 == 0) {
                    String msg = String.format("Bad data [%d]...", event.f1);
                    log.error(msg);
                    // 抛出异常，作业根据 配置 的重启策略进行恢复，无重启策略作业直接退出。
                    throw new RuntimeException(msg);
                }
                return new Tuple3<>(event.f0, event.f1, new Timestamp(System.currentTimeMillis()).toString());
            }
        }).print();

        env.execute();


    }
}
