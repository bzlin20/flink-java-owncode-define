package com.ds.flink.core.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @ClassName kafkaSource
 * @Description  flink 读取kafka的数据
 * @Author ds-longju
 * @Date 2022/7/17 9:15 下午
 * @Version 1.0
 **/
public class kafkaSource {
    public static void main(String[] args) throws Exception  {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", "120.26.126.158:9092,120.26.126.158:9093,120.26.126.158:9094");
        props.put("zookeeper.connect", "120.26.126.158:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        DataStreamSource<String> metric = env.addSource(new FlinkKafkaConsumer<String>(
                "metric",  //kafka topic
                new SimpleStringSchema(),  // String 序列化
                props
        )).setParallelism(1);

        metric.print();

        env.execute("Flink add data source");

    }


}
