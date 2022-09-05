package com.ds.flink.core.faulttolerance.Topology;

import com.ds.flink.core.faulttolerance.Topology.function.SideOutputProcessFunction;
import com.ds.flink.core.faulttolerance.Topology.function.SimpleSourceFunction;
import com.ds.flink.core.faulttolerance.Topology.function.StateProcessFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName: TopologyChanges
 * @Description: 演示业务需要进行topology变化时候，如何升级。
 * @opt :  1、创建savepoint并停止作业：  bin/flink cancel -s 834b7e0c414ac7d4c771c13bdbcbcf60
 *         2、携带--allowNonRestoredState，启动：bin/flink run -d -m localhost:4000 -s file:///tmp/chkdir/savepoint-2bba7a-cc9506fd8052 --allowNonRestoredState
 * @author: ds-longju
 * @Date: 2022-09-04 09:42
 * @Version 1.0
 **/
public class TopologyChanges {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final OutputTag<Tuple3<String, Integer, Long>> outputTag =
                new OutputTag<Tuple3<String, Integer, Long>>("side-output") {};

        // 打开Checkpoint
        env.enableCheckpointing(20);


        // 业务版本3
        version3(env, outputTag);
        // 业务版本4
//        version4(env);

        // 大家可以在 https://flink.apache.org/visualizer/ 生成拓扑图
        String planJson = env.getExecutionPlan();

        env.execute("TopologyChanges");

    }
    /**
     * 第三个业务版本 加了个uid来保证算子的唯一标识
     */
    private static void version3(StreamExecutionEnvironment env, OutputTag<Tuple3<String, Integer, Long>> outputTag) {
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> mainResult = env
                .addSource(new SimpleSourceFunction())
                .keyBy(0)
                .sum(1).uid("khkw_sum")
                .process(new SideOutputProcessFunction<>(outputTag));
        mainResult.print().name("main-result");
        mainResult.getSideOutput(outputTag).print().name("side-output");
    }


    /***
     * 拓扑图发生改变后的计算结果基于上次version3的savepoint的state进行计算
     * desc: 相对于version3 来说新增StateProcessFunction 这个节点逻辑，去掉 SideOutputProcessFunction
     * 第四个业务版本
     * @param env
     */
    private static void version4(StreamExecutionEnvironment env) {
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> mainResult = env
                .addSource(new SimpleSourceFunction())
                .keyBy(0)
                .process(new StateProcessFunction())
                .keyBy(0)
                .sum(1).uid("khkw_sum");
        mainResult.print().name("main-result");
    }
}
