package com.ds.flink.core.test;


import com.alibaba.fastjson.JSON;
import com.ds.flink.core.model.Student;
import com.ds.flink.core.sink.SinkToMySQL;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @ClassName Main3
 * @Description kafka 数据落到mysql mian程序
 * @Author ds-longju
 * @Date 2022/7/17 9:51 下午
 * @Version 1.0
 **/
public class Main3 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", "120.26.126.158:9092,120.26.126.158:9093,120.26.126.158:9094");
        props.put("zookeeper.connect", "120.26.126.158:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer<String>(
                "student", new SimpleStringSchema(), props
        )).map(string -> JSON.parseObject(string, Student.class));


        SingleOutputStreamOperator<Student> map = student.map(new MapFunction<Student, Student>() {
            @Override
            public Student map(Student value) throws Exception {
                Student s1 = new Student();
                s1.id = 1;
                s1.name = "longju";
                s1.password = value.password;
                s1.age = value.age + 5;
                return null;
            }
        });
        map.print();

        student.addSink(new SinkToMySQL());  // 输出到mysql

        env.execute("Flink add sink");

    }



}
