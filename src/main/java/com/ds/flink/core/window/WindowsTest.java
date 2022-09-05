package com.ds.flink.core.window;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.List;

/**
 * @ClassName 测试windos的使用
 * @Description window 样例
 * @Author ds-longju
 * @Date 2022/7/29 2:53 下午
 * @Version 1.0
 **/
public class WindowsTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> sourceDataStream = env.socketTextStream("localhost", 7777);

        // 注册eventTime 字段
        SerializableTimestampAssigner<String> timestampAssigner = new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                String[] fields = element.split(",");
                Long aLong = new Long(fields[0]);
                return aLong * 1000L;
            }
        };
        SingleOutputStreamOperator<JSONObject> map = sourceDataStream.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                JSONObject Person = new JSONObject();
                Timestamp time1 = new Timestamp(System.currentTimeMillis());
                Person.put("name",s);
                Person.put("eventTime",time1);
                return new JSONObject();
            }
        });

        // windowsAll的使用
        SingleOutputStreamOperator<List<JSONObject>> eventTime = map.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(4L))
                        // watermark延迟设置时间，并定义时间提取函数
                        .withTimestampAssigner((jsonObj, timestamp) -> jsonObj.getLong("eventTime")))
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10L), Time.seconds(5L)))
                .apply(new RecordToFrameByWindow());


    }
}
