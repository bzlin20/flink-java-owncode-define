package com.ds.flink.core.window;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import java.time.Duration;

/**
 * @ClassName WatermarkWCDemo
 * @Description 请描述类的业务用途
 * @Author ds-longju
 * @Date 2022/7/29 3:28 下午
 * @Version 1.0
 **/
public class WatermarkWCDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        // 注册eventTime 字段
        SerializableTimestampAssigner<String> timestampAssigner = new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                String[] fields = element.split(",");
                Long aLong = new Long(fields[0]);
                return aLong * 1000L;
            }
        };
        SingleOutputStreamOperator<String> watermarkStream = inputStream.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(timestampAssigner));
        watermarkStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = value.split(",");
                out.collect(new Tuple2<>(fields[1], 1));
            }
        }).keyBy(data -> data.f0).window(TumblingEventTimeWindows.of(Time.seconds(10))).sum(1).print();

        env.execute("run watermark wc");


    }
}
