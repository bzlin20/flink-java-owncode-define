package com.ds.flink.core.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @ClassName TimeWindowExample
 * @Description window 样例
 * @Author ds-longju
 * @Date 2022/7/29 2:53 下午
 * @Version 1.0
 *
 * // Keyed Window
 * stream
 *        .keyBy(...)               <-  按照一个Key进行分组
 *        .window(...)              <-  将数据流中的元素分配到相应的窗口中
 *       [.trigger(...)]            <-  指定触发器Trigger（可选）
 *       [.evictor(...)]            <-  指定清除器Evictor(可选)
 *        .reduce/aggregate/process()      <-  窗口处理函数Window Function
 *
 * // Non-Keyed Window
 * stream
 *        .windowAll(...)           <-  不分组，将数据流中的所有元素分配到相应的窗口中
 *       [.trigger(...)]            <-  指定触发器Trigger（可选）
 *       [.evictor(...)]            <-  指定清除器Evictor(可选)
 *        .reduce/aggregate/process()      <-  窗口处理函数Window Function
 **/
public class TimeWindowExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("daidai", 9999);



        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] strings = s.split(" ");
                for (String string : strings) {
                    collector.collect(Tuple2.of(string, 1));
                }
            }
        });
//        SingleOutputStreamOperator<String> watermarkStream = wordAndOne.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(timestampAssigner));


        //滚动窗口
        SingleOutputStreamOperator<Tuple2<String, Integer>> result1 = wordAndOne
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);
        //滑动窗口
        SingleOutputStreamOperator<Tuple2<String, Integer>> result2 = wordAndOne
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(3)))
                .sum(1);

        //计数窗口
        SingleOutputStreamOperator<Tuple2<String, Integer>> result3 = wordAndOne
                .keyBy(t -> t.f0)
                .countWindow(5L)
                .sum(1);

        //回话窗口
        SingleOutputStreamOperator<Tuple2<String, Integer>> result4 = wordAndOne
                .keyBy(t -> t.f0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .sum(1);


        result1.print("滚动");
        result2.print("滑动");
        result3.print("计数");
        result4.print("会话");

    }
}
