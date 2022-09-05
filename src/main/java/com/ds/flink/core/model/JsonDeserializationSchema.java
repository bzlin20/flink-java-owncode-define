package com.ds.flink.core.model;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class JsonDeserializationSchema  implements DebeziumDeserializationSchema<String>{
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //创建JSON对象用于封装结果数据
        JSONObject result = new JSONObject();

        //获取库名&表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        result.put("db", fields[1]);
        result.put("tableName", fields[2]);

        //获取before数据
        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null) {
            //获取列信息
            Schema schema = before.schema();
            List<Field> fieldList = schema.fields();

            for (Field field : fieldList) {
                beforeJson.put(field.name(), before.get(field));
            }
        }
        result.put("before", beforeJson);

        //获取after数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null) {
            //获取列信息
            Schema schema = after.schema();
            List<Field> fieldList = schema.fields();

            for (Field field : fieldList) {
                afterJson.put(field.name(), after.get(field));
            }
        }
        result.put("after", afterJson);

        // 获取操作时间
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        Date now = new Date();
        Long ts = value.getInt64("ts_ms");
        String time = sdf.format(ts);
        result.put("ts",time);

        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        // 转换READ 为INSERT
        result.put("op", operation.toString() =="READ" || operation.toString() =="CREATE" ? "INSERT" : operation);



        //输出数据
        collector.collect(result.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
