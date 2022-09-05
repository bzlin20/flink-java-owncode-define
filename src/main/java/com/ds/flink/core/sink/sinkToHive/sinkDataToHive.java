package com.ds.flink.core.sink.sinkToHive;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @ClassName:
 * @Description:
 * @author: ds-longju
 * @Date: 2022-08-30 11:08
 * @Version 1.0
 **/
public class sinkDataToHive {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                String str = "test" + "," + Math.random();
                sourceContext.collect(str);
            }

            @Override
            public void cancel() {
            }
        });

        stringDataStreamSource.print();
//        stringDataStreamSource.addSink(new HiveWriter());

        env.execute("FLINK SINK TO HIVE");
    }
}
