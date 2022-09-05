package com.ds.flink.core.opeator.topN;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Properties;

/**
 * @ClassName TopNJob
 * @Description topN 主类
 * @Author ds-longju
 * @Date 2022/8/3 3:11 下午
 * @Version 1.0
 **/
public class TopNJob {
    //最对延迟到达的时间
    public static final long MAX_EVENT_DELAY = 10L;

    public static void main(String[] args) throws Exception {
        //构建流执行环境
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度1，方便打印
        env.setParallelism(1);

        /** ProcessingTime：事件被处理的时间。也就是由机器的系统时间来决定。
         EventTime：事件发生的时间。一般就是数据本身携带的时间。
         */
        //设置下eventTime，默认为processTime即系统处理时间，我们需要统计一小时内的数据，也就是数据带的时间eventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //kafka
        Properties prop = new Properties();
        prop.put("bootstrap.servers", KafkaWriter.BROKER_LIST);
        prop.put("zookeeper.connect", "120.26.126.158:2181");
        prop.put("group.id", KafkaWriter.TOPIC_USER_ACTION);
        prop.put("key.serializer", KafkaWriter.KEY_SERIALIZER);
        prop.put("value.serializer", KafkaWriter.VALUE_SERIALIZER);
        prop.put("auto.offset.reset", "latest");

        // 新增数据源 kafka
        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<>(
                KafkaWriter.TOPIC_USER_ACTION,
                new SimpleStringSchema(),
                prop
        ));

        //从kafka里读取数据，转换成UserAction对象
        DataStream<UserAction> dataStream = dataStreamSource.map(value -> JSONObject.parseObject(value, UserAction.class));

//        dataStream.print();
        //将乱序的数据进行抽取出来，设置watermark，数据如果晚到10秒的会被丢弃
        DataStream<UserAction> timedData = dataStream.assignTimestampsAndWatermarks(new UserActionTSExtractor());

        //为了统计5分钟购买的最多的，所以我们需要过滤出购买的行为
        DataStream<UserAction> filterData = timedData.filter(new FilterFunction<UserAction>() {
            @Override
            public boolean filter(UserAction userAction) throws Exception {
                return userAction.getBehavior().contains("buy");
            }
        });

        //窗口统计点击量 滑动的窗口 2分钟一次  统计10分钟最高的  比如 [09:00, 10:00), [09:05, 10:05), [09:10, 10:10)…
        DataStream<ItemBuyCount> windowedData = filterData
                .keyBy("itemId")
                .timeWindow(Time.minutes(10L), Time.minutes(2L))
                .aggregate(new CountAgg(), new WindowResultFunciton());

        //Top N 计算最热门的商品
        SingleOutputStreamOperator<List<ItemBuyCount>> windowEnd = windowedData.keyBy("windowEnd").process(new TopNHotItems(3));

        // 输出数据到mysql
//        windowEnd.addSink(new MySqlSink());

        // 打印数据
        windowEnd.flatMap(new FlatMapFunction<List<ItemBuyCount>, String>() {
            @Override
            public void flatMap(List<ItemBuyCount> ItemBuyCounts, Collector<String> collector) throws Exception {
                StringBuffer stringBuffer = new StringBuffer();
                for(ItemBuyCount value:ItemBuyCounts){
                    stringBuffer.append(value.toString());

                }
            }
        }).print();

        env.execute("Top N Job");




    }

    /**
     * 用于行为时间戳抽取器，最多十秒延迟，也就是晚到10秒的数据会被丢弃掉
     */
    public static class UserActionTSExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserAction> {

        public UserActionTSExtractor() {
            super(Time.seconds(MAX_EVENT_DELAY));
        }

        @Override
        public long extractTimestamp(UserAction userAction) {
            return userAction.getTimestamp();
        }
    }

    /**
     *
     * COUNT 聚合函数实现，每出现一条记录加一。AggregateFunction<输入，汇总，输出>
     */
    public static class CountAgg implements AggregateFunction<UserAction, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserAction userAction, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }

    /**
     * 用于输出结果的窗口WindowFunction<输入，输出，键，窗口>
     */
    public static class WindowResultFunciton implements WindowFunction<Long, ItemBuyCount, Tuple, TimeWindow> {

        @Override
        public void apply(
                Tuple key, //窗口主键即itemId
                TimeWindow window, //窗口
                Iterable<Long> aggregationResult, //集合函数的结果，即count的值
                Collector<ItemBuyCount> collector //输出类型collector
        ) throws Exception {
            Long itemId = ((Tuple1<Long>) key).f0;
            Long count =aggregationResult.iterator().next();
            collector.collect(ItemBuyCount.of(itemId, window.getEnd(), count));

        }
    }





}
