package com.ds.flink.core.faulttolerance.restartjob;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;


/**
 * @ClassName:  FixedDelayRestartJob
 * @Description: Flink重启策略之固定延迟重启策略
 * @author: ds-longju
 * @Date: 2022-09-01 17:11
 * @Version 1.0
 **/
public class FixedDelayRestartJob {
    public static void main(String[] args) throws Exception {
        Logger log = LoggerFactory.getLogger(FixedDelayRestartJob.class);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000 ));

//        env.setRestartStrategy(
//                RestartStrategies.fixedDelayRestart(3, Time.of(3, TimeUnit.SECONDS) ));

        DataStream<Tuple3<String, Integer, Long>> source = env
                .addSource(new SourceFunction<Tuple3<String, Integer, Long>>() {
                    @Override
                    public void run(SourceContext<Tuple3<String, Integer, Long>> ctx) throws Exception {
                        int index = 1;
                        while(true){
                            ctx.collect(new Tuple3<>("key", index++, System.currentTimeMillis()));
                            // Just for testing
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

        env.execute("FixedDelayRestart");

    }
}
