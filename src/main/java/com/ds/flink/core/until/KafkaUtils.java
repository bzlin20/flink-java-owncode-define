package com.ds.flink.core.until;

import com.alibaba.fastjson.JSON;
import com.ds.flink.core.model.Metric;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/***
 * @autor : ds-longju
 * @date :
 * @desc : 往kafka生产数据
 */
public class KafkaUtils {
    public static final  String broker_list = "120.26.126.158:9092" ; // kafka 连接地址
    public static final  String topic = "metric";   //kafka topic，Flink 程序中需要和这个统一

    public static void writeToKafka() throws InterruptedException{
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        Metric metric = new Metric();
        metric.setTimestamp(System.currentTimeMillis());
        metric.setName("mem");
        Map<String, String> tags = new HashMap<String,String>();
        Map<String, Object> fields = new HashMap<String,Object>();

        tags.put("cluster", "longju");
        tags.put("host_ip", "101.147.022.106");

        fields.put("used_percent", 90d);
        fields.put("max", 27244873d);
        fields.put("used", 17244873d);
        fields.put("init", 27244873d);

        metric.setTags(tags);
        metric.setFields(fields);

        ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(metric));
        producer.send(record);
        System.out.println("发送数据: " + JSON.toJSONString(metric));

        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        while (true){
            Thread.sleep(300);
            writeToKafka();
        }
    }

}
