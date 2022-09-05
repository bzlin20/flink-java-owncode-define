package com.ds.flink.core.transformation;

import com.alibaba.fastjson.JSON;
import com.ds.flink.core.model.Student;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @ClassName FlinkTransformationTest
 * @Description 请描述类的业务用途
 * @Author ds-longju
 * @Date 2022/7/17 10:15 下午
 * @Version 1.0
 **/
public class FlinkTransformationTest {
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
        )).map(String -> JSON.parseObject(String, Student.class));

        // 自定义map
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


        // 自定义FlatMap
        SingleOutputStreamOperator<Student> flatmap = student.flatMap(new FlatMapFunction<Student, Student>() {
            @Override
            public void flatMap(Student value, Collector<Student> out) throws Exception {
                if (value.id % 2 == 0) {
                    out.collect(value);
                }
            }
        });
        flatmap.print();

        // 自定义 filter
        SingleOutputStreamOperator<Student> filter = student.filter(new FilterFunction<Student>() {
            @Override
            public boolean filter(Student value) throws Exception {
                if (value.id > 95) {
                    return true;
                }
                return false;
            }
        });
        filter.print();


        // 自定义keyby KeyBy 在逻辑上是基于 key 对流进行分区。在内部，它使用 hash 函数对流进行分区。它返回 KeyedDataStream 数据流。

        KeyedStream<Student, Integer> studentIntegerKeyedStream = student.keyBy(new KeySelector<Student, Integer>() {
            @Override
            public Integer getKey(Student student) throws Exception {
                return student.age;
            }
        });



        // reduce  Reduce 返回单个的结果值，并且 reduce 操作每处理一个元素总是创建一个新值。常用的方法有 average, sum, min, max, count，使用 reduce 方法都可实现。
        SingleOutputStreamOperator<Student> reduce = studentIntegerKeyedStream.reduce(new ReduceFunction<Student>() {
            @Override
            public Student reduce(Student value1, Student value2) throws Exception {
                Student student1 = new Student();
                student1.name = value1.name + value2.name;
                student1.id = (value1.id + value2.id) / 2;
                student1.password = value1.password + value2.password;
                student1.age = (value1.age + value2.age) / 2;
                return student1;
            }
        });

        reduce.print();

        // fold Fold 通过将最后一个文件夹流与当前记录组合来推出 KeyedStream。
//        KeyedStream.fold("1", new FoldFunction<Integer, String>() {
//            @Override
//            public String fold(String accumulator, Integer value) throws Exception {
//                return accumulator + "=" + value;
//            }
//        })

        // Aggregations DataStream API 支持各种聚合，例如 min，max，sum 等。 这些函数可以应用于 KeyedStream 以获得 Aggregations 聚合。
//        KeyedStream.sum(0)
//        KeyedStream.sum("key")
//        KeyedStream.min(0)
//        KeyedStream.min("key")
//        KeyedStream.max(0)
//        KeyedStream.max("key")
//        KeyedStream.minBy(0)
//        KeyedStream.minBy("key")
//        KeyedStream.maxBy(0)
//        KeyedStream.maxBy("key")

        // Window Window 函数允许按时间或其他条件对现有 KeyedStream 进行分组。 以下是以 10 秒的时间窗口聚合：
//        inputStream.keyBy(0).window(Time.seconds(10));

        // WindowAll
//        inputStream.keyBy(0).windowAll(Time.seconds(10));

//        在keyby后数据分流，window是把不同的key分开聚合成窗口，而windowall则把所有的key都聚合起来所以windowall的并行度只能为1，而window可以有多个并行度。

//        Union Union 函数将两个或多个数据流结合在一起。 这样就可以并行地组合数据流。 如果我们将一个流与自身组合，那么它会输出每个记录两次。
//        inputStream.union(inputStream1, inputStream2, ...);

//        Window join 我们可以通过一些 key 将同一个 window 的两个数据流 join 起来。
//        inputStream.join(inputStream1)
//                .where(0).equalTo(1)
//                .window(Time.seconds(5))
//                .apply (new JoinFunction () {...});

//        Split 此功能根据条件将流拆分为两个或多个流。 当您获得混合流并且您可能希望单独处理每个数据流时，可以使用此方法。
//        SplitStream<Integer> split = inputStream.split(new OutputSelector<Integer>() {
//            @Override
//            public Iterable<String> select(Integer value) {
//                List<String> output = new ArrayList<String>();
//                if (value % 2 == 0) {
//                    output.add("even");
//                }
//                else {
//                    output.add("odd");
//                }
//                return output;
//            }
//        });





        env.execute("Flink transformation test");





    }
}
