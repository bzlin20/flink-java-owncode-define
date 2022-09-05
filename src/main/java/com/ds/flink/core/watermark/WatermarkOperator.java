package com.ds.flink.core.watermark;

import com.alibaba.fastjson.JSON;
import com.ds.flink.core.model.Student;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Properties;

/**
 * @ClassName WatermarkOperator
 * @Description watermark操作
 * @Author ds-longju
 * @Date 2022/7/24 11:45 上午
 * @Version flink 1.13
 * @use
 **/
public class WatermarkOperator {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", "120.26.126.158:9092,120.26.126.158:9093,120.26.126.158:9094");
        props.put("zookeeper.connect", "120.26.126.158:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        SingleOutputStreamOperator<Student> studentStream = env.addSource(new FlinkKafkaConsumer<String>(
                "student", new SimpleStringSchema(), props
        )).map(String -> JSON.parseObject(String, Student.class));

        /***
         * 老版本设置水位线
         */
        // 设置时间属性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 注册waterMark
        SingleOutputStreamOperator<Student> studentSingleOutputStreamOperator = studentStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Student>() {

            long currentTimeStamp = 0l;
            //修改最大乱序时间
            long maxDelayAllowed = 5*100l;
            long currentWaterMark;

            @Override
            public long extractTimestamp(Student student, long l) {
                String[] arr = student.getName().split(",");
                long timeStamp = Long.parseLong(arr[1]);
                // 更新最大的时间戳
                currentTimeStamp = Math.max(timeStamp, currentTimeStamp);
                // 返回记录的时间戳
                return timeStamp;

            }
            @Override
            public Watermark getCurrentWatermark() {
                //生成具有2s容忍度的水位线
                currentWaterMark = currentTimeStamp - maxDelayAllowed;
                return new Watermark(currentWaterMark);
            }
        });

        /***
         *  flink 1.13 waterMark
         *  maxOutOfOrderness 参数，表示“最大乱序程度”
         */
        WatermarkStrategy<Student> watermarkStrategy = WatermarkStrategy.<Student>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Student>() {
                    //返回指定的字段作为水印字段，这里设置为5秒延迟
                    @Override
                    public long extractTimestamp(Student element, long recordTimestamp) {
                        return Long.parseLong(element.getName());
                    }
                });
        SingleOutputStreamOperator<Student> studentSingleOutputStreamOperator1 = studentStream.assignTimestampsAndWatermarks(watermarkStrategy);




    }
}
