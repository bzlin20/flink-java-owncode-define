package com.ds.flink.core.test;


import com.alibaba.fastjson.JSONObject;
import com.ds.flink.core.mapFunction.DimSync;
import com.ds.flink.core.mapFunction.JDBCAsyncFunction;
import com.ds.flink.core.mapFunction.WholeLoad;
import com.ds.flink.core.model.JsonDeserializationSchema;
import com.ds.flink.core.model.fplOverview;
import com.ds.flink.core.source.SourceFromMysqlFq;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import io.vertx.core.json.JsonObject;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName Main1
 * @Description binlog 数据关联 mysql维表
 * @Author ds-longju
 * @Date 2022/7/19 3:44 下午
 * @Version 1.0
 **/
public class Main1 {
    public  static String getYearWeek(String s ) throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = df.parse(s);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int week = cal.get(Calendar.WEEK_OF_YEAR);
        int year = cal.get(Calendar.YEAR) - 2;
        return year + "" + week ;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("rm-bp161be65d56kbt4nzo.mysql.rds.aliyuncs.com")
                .port(3306)
                .username("root")
                .password("qyllt1314#")
                .databaseList("mysqldb")
                .tableList("mysqldb.fpl_overview")
                .startupOptions(StartupOptions.latest())
                .deserializer(new JsonDeserializationSchema())
                .build()
                ;
        // 创建两个流
        DataStreamSource<String> stringDataStreamSource = env.addSource(sourceFunction);
        DataStreamSource<fplOverview> fplOverviewDataStreamSource = env.addSource(new SourceFromMysqlFq());


        // 将binlog数据转成json字符串
        SingleOutputStreamOperator<JSONObject> map = stringDataStreamSource.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSONObject.parseObject(s);
            }
        });

        // 删除delete 的数据
        SingleOutputStreamOperator<JSONObject> filterStream = map.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                if (jsonObject.getString("op").equals("DELETE")) {
                    return false;
                }
                return true;
            }
        });

        // 加工 yw字段
        SingleOutputStreamOperator<fplOverview> mapFplOverviewStream = filterStream.map(new MapFunction<JSONObject, fplOverview>() {
            @Override
            public fplOverview map(JSONObject jsonObject) throws Exception {
                // INSERT 和UPDATE 都取after之后的数据
                JSONObject after = JSONObject.parseObject(jsonObject.getString("after"));
                return new fplOverview(
                        after.getString("id"),
                        after.getString("dp_id"),
                        after.getString("datatime"),
                        after.getString("fpl_amount"),
                        getYearWeek(after.getString("datatime"))
                );
            }
        });

        // 实时关联维表
        mapFplOverviewStream.map(new DimSync()).print();

//        //预加载全量数据
//        mapFplOverviewStream.map(new WholeLoad()).print();

        // Aync 异步加载数据  10000min 更新
//        SingleOutputStreamOperator<JsonObject> jsonObjectSingleOutputStreamOperator = AsyncDataStream.unorderedWait(mapFplOverviewStream, new JDBCAsyncFunction(), 10000, TimeUnit.MILLISECONDS, 100);
//
//        jsonObjectSingleOutputStreamOperator.print();


        // 双流做join
//        mapFplOverviewStream.coGroup(fplOverviewDataStreamSource).where(new KeySelector<fplOverview, String>() {
//            @Override
//            public String getKey(fplOverview fplOverview) throws Exception {
//                return fplOverview.getDp_id();
//            }
//        }).equalTo(new KeySelector<fplOverview, String>() {
//            @Override
//            public String getKey(fplOverview fplOverview) throws Exception {
//                return fplOverview.getDp_id();
//            }
//        }).window(ProcessingTimeSessionWindows.withGap(Time.seconds(2)))
//                .apply(
//                new CoGroupFunction<fplOverview,fplOverview, String>(){
//                    @Override
//                    public void coGroup(Iterable<fplOverview> first, Iterable<fplOverview> second, Collector<String> out) throws Exception {
//                        out.collect(first.toString() + second.toString());
//                    }
//                }
//        ).print();





        env.execute("source from mysql");


    }

}
