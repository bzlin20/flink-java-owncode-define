package com.ds.flink.core.faulttolerance.Correctness;

import com.ds.flink.core.faulttolerance.Correctness.functions.MapFunctionWithException;
import com.ds.flink.core.faulttolerance.Correctness.functions.NonParallelCheckpointedSource;
import com.ds.flink.core.faulttolerance.Correctness.functions.Tuple3KeySelector;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @ClassName: CheckpointedSourceTestCase
 * @Description: 主类
 * @author: ds-longju
 * @Date: 2022-09-05 14:26
 * @Version 1.0
 **/
public class CheckpointedSourceTestCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置重启策略
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(3, Time.of(2, TimeUnit.SECONDS)));

        env.enableCheckpointing(20);

        nonParallel(env);
//        parallel(env);
//        parallelFromTaskIndex(env);

        env.execute("NonParallelCheckpointedSource");
    }

    private static void nonParallel(StreamExecutionEnvironment env) {
        env.setParallelism(1);
        env.addSource(new NonParallelCheckpointedSource())
                .map(new MapFunctionWithException())
                .keyBy(new Tuple3KeySelector())
                .sum(1).print();
    }

//
//    private static void parallel(StreamExecutionEnvironment env) {
//        env.setParallelism(2);
//        env.addSource(new ParallelCheckpointedSource())
//                .map(new MapFunctionWithException())
//                .keyBy(new Tuple3KeySelector())
//                .sum(1).print();
//    }
//
//    private static void parallelFromTaskIndex(StreamExecutionEnvironment env) {
//        env.setParallelism(2);
//        env.addSource(new ParallelCheckpointedSourceRestoreFromTaskIndex())
//                .map(new MapFunctionWithException())
//                .keyBy(new Tuple3KeySelector())
//                .sum(1).print();
//    }


}
