package com.ds.flink.core.faulttolerance.upgrade;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;


/**
 * @ClassName: KeepCheckpointForRestore
 * @Description: 开启Checkpoint之后,Flink默认会在停止作业时候删除Checkpoint文件，某些情况我们期望保留Checkpoint文件，用于回复作业。
 * @author: ds-longju
 * @Date: 2022-09-02 15:41
 * @Version 1.0
 **/
public class KeepCheckpointForRestore {
    public static void main(String[] args) throws Exception {
        Logger log = LoggerFactory.getLogger(KeepCheckpointForRestore.class);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置重启策略
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(3, Time.of(2, TimeUnit.SECONDS) ));

        // 打开Checkpoint
        env.enableCheckpointing(20);
        //开启状态后端存储数据，默认checkpoint存储在jobmanager里
        env.setStateBackend(new FsStateBackend("file:///Users/longju/Desktop/temp/chkdir",false));

        // 作业失败保留cp，可以从对应的checkpoint文件中恢复作业
//        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION表示取消作业是保留检查点，这里说的取消是正常取消不是任务失败。
//                    如果重启任务，检查点不会自动清除，如果需要清除则需要手动清除。
//        ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION表示取消作业时删除检查点，如果任务失败，检查点不会删除。
//                    也就是说任务失败可以从检查点恢复任务。

        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
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
        }).keyBy(0).sum(1).print();

        env.execute("CheckpointForFailover");


    }
}
