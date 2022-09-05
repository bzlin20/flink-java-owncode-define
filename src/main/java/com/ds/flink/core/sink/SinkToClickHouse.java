package com.ds.flink.core.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ds.flink.core.model.Student;
import com.ds.flink.core.until.ClickHouseUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @ClassName: SinkToClickHouse
 * @Description: kafka数据写到clickhouse
 * @author: ds-longju
 * @Date: 2022-08-19 10:58
 * @Version 1.0
 **/
public class SinkToClickHouse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", "120.26.126.158:9092,");
        props.put("zookeeper.connect", "120.26.126.158:2181");
        props.put("group.id", "student-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest"); // latest


        DataStreamSource<String> stringStream = env.addSource(new FlinkKafkaConsumer<String>(
                "student",  //kafka topic
                new SimpleStringSchema(),  // String 序列化
                props
        ));
        SingleOutputStreamOperator<Student> studentStream = stringStream.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                int id = Integer.parseInt(jsonObject.getString("id"));
                String name = jsonObject.getString("name");
                String password = jsonObject.getString("password");
                int age = Integer.parseInt(jsonObject.getString("age"));
                return new Student(id, name, password, age);
            }
        });
        // sink
        studentStream.print();
        String sql = "INSERT INTO default.ods_student (id,name,password,age) " +
                "VALUES (?, ?, ?, ?)";
        studentStream.addSink(new ClickHouseUtil(sql));

        env.execute("Kafka sink ClickHouse") ;

    }
}
