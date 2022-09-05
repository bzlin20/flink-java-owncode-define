package com.ds.flink.core.sink;

import com.alibaba.fastjson.JSONObject;
import com.ds.flink.core.model.Student;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import java.util.Properties;

/**
 * @ClassName SinkToKafka
 * @Description flink group by 求和 timewindow
 * @Author ds-longju
 * @Date 2022/7/21 7:20 下午
 * @Version 1.0
 **/
public class SinkToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        Properties props = new Properties();
        props.put("bootstrap.servers", "120.26.126.158:9092,120.26.126.158:9093,120.26.126.158:9094");
        props.put("zookeeper.connect", "120.26.126.158:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        
        DataStreamSource<String> metricDataStream = env.addSource(new FlinkKafkaConsumer<String>(
                "student",  //kafka topic
                new SimpleStringSchema(),  // String 序列化
                props
        )).setParallelism(1);

        // 处理成Student 
        SingleOutputStreamOperator<Student> StudentMap = metricDataStream.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String s) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                Integer id = jsonObject.getInteger("id");
                String name = jsonObject.getString("name");
                String password = jsonObject.getString("password");
                Integer age = jsonObject.getInteger("age");
                return new Student(id, name, password, age);
            }
        });

        // 处理成tuple
        SingleOutputStreamOperator<Tuple2<String, Integer>> StudentMap2 = StudentMap.map(new MapFunction<Student, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Student student) throws Exception {
                return new Tuple2(student.getName(), student.getAge());
            }
        });
        // keyby sum
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = StudentMap2.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        }).sum(1);

        // toString
        SingleOutputStreamOperator<String> map = sum.map(new MapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.toString();
            }
        });
        map.print();

//        SingleOutputStreamOperator<Student> reduce = StudentMap.keyBy(Student -> Student.getId()).reduce(new ReduceFunction<Student>() {
//            @Override
//            public Student reduce(Student t1, Student t2) throws Exception {
//                return new Student(t1.getId(), t1.getName(), t1.getPassword(), t1.getAge() + t2.getAge());
//            }
//        });

        Properties pproperties = new Properties();
        pproperties.put("bootstrap.servers", "120.26.126.158:9092,120.26.126.158:9093,120.26.126.158:9094");
        pproperties.put("zookeeper.connect", "120.26.126.158:2181");
        pproperties.put("group.id", "metric-group");


//        SingleOutputStreamOperator<String> map = reduce.map(new MapFunction<Student, String>() {
//            @Override
//            public String map(Student student) throws Exception {
//                return student.toString();
//            }
//        });

        // 输出到kafka
        FlinkKafkaProducer<String> producer =
                new FlinkKafkaProducer<String>("sink_student", (KafkaSerializationSchema<String>) new SimpleStringSchema(), pproperties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        map.addSink(producer);


        env.execute("Flink Kafka to Kafka") ; 
        
    }
}
