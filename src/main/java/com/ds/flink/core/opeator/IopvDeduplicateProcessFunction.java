package com.ds.flink.core.opeator;

import com.ds.flink.core.model.fplOverview;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @ClassName IopvDeduplicateProcessFunction
 * @Description  根据fplOverview的ID去重
 * @Author ds-longju
 * @Date 2022/8/3 10:39 上午
 * @Version 1.0
 **/
public class IopvDeduplicateProcessFunction extends RichFlatMapFunction<fplOverview, fplOverview>{
    //定义MapState
    private MapState<String, fplOverview> mapState;

    //状态初始化
    @Override
    public void open(Configuration parameters) throws Exception {
        // 定义状态的过期时间为3s
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(3))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        // 定义 mapState
        MapStateDescriptor descriptor = new MapStateDescriptor("MapDescriptor", String.class, fplOverview.class);
        // 设置生命周期
        descriptor.enableTimeToLive(ttlConfig);
        mapState = getRuntimeContext().getMapState(descriptor);
    }

    // 利用flastMap 进行去重，把 key-vale 存到mapState 中，当 新元素过来在状态中能匹配到value，说明有重复的，不输出
    @Override
    public void flatMap(fplOverview f, Collector<fplOverview> collector) throws Exception {
        String id = f.getId();
        // 去重
        if (mapState.get(id) == null) {
            mapState.put(id, f);
            collector.collect(f);
        }

    }
}
