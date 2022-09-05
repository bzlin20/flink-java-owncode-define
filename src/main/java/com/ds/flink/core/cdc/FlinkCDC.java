package com.ds.flink.core.cdc;

import com.ds.flink.core.model.JsonDeserializationSchema;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * flink cdc 读取mysql 的binlog
 * initial 从binlog头读取,然后增量读取
 * @author :ds-longju
 * @date: 20220717
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception  {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(20);

        //2.通过FlinkCDC构建SourceFunction
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("rm-bp161be65d56kbt4nzo.mysql.rds.aliyuncs.com")
                .port(3306)
                .username("root")
                .password("qyllt1314#")
                .databaseList("mysqldb")
                .tableList("mysqldb.rds_source")
//                .deserializer(new StringDebeziumDeserializationSchema())   //默认
                .deserializer(new JsonDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                // 启动参数 提供了如下几个静态方法
                // StartupOptions.initial() 第一次启动的时候，会把历史数据读过来（全量）做快照，后续读取binlog加载新的数据，如果不做 chackpoint 会存在重启又全量一遍。
                // StartupOptions.earliest() 只从binlog开始的位置读（源头），这里注意，如果binlog开启的时间比你建库时间晚，可能会读不到建库语句会报错，earliest要求能读到建表语句
                // StartupOptions.latest() 只从binlog最新的位置开始读
                // StartupOptions.specificOffset() 自指定从binlog的什么位置开始读
                // StartupOptions.timestamp() 自指定binlog的开始时间戳
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        // 3.数据打印
        dataStreamSource.print();

        // 4. 启动任务
        env.execute("Flink CDC");
    }
}
