package com.ds.flink.core.until;

import com.alibaba.fastjson.JSON;
import com.ds.flink.core.model.Student;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @ClassName KafkaUtils2
 * @Description 往kafka中写数据 topic student
 * @Author ds-longju
 * @Date 2022/7/17 9:47 下午
 * @Version 1.0
 **/
public class KafkaUtils2 {
    public static final String broker_list = "120.26.126.158:9092";
    public static final String topic = "student";  //kafka topic 需要和 flink 程序用同一个 topic

    public static void writeToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        for (int i = 1; i <= 100; i++) {
            Thread.sleep(2000);
            Student student = new Student(i, "longju", "password" + i, 18 + i);
            ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(student));
            producer.send(record);
            System.out.println("发送数据: " + JSON.toJSONString(student));
        }
        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        writeToKafka();
    }

}
