package com.ds.flink.core.window;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @ClassName windows 窗口去重
 * @Description window 样例
 * @Author ds-longju
 * @Date 2022/7/29 2:53 下午
 * @Version 1.0
 * @use
 *         SingleOutputStreamOperator<List < JSONObject>> clxtList = clxt.process(new MapAndFilterByTimestamp())
 *                 // watermark延迟设置时间，并定义时间提取函数
 *                 .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(delay))
 *                         .withTimestampAssigner((jsonObj, timestamp) -> jsonObj.getLong("eventTime")))
 *                 // 开事件时间 滑动窗口
 *                 .windowAll(SlidingEventTimeWindows.of(Time.seconds(windowLen), Time.seconds(stepLen)))
 *                 // 使用事件时间滑动窗口将数据转为帧数据
 *                 .apply(new RecordToFrameByWindow());
 */
public class RecordToFrameByWindow implements AllWindowFunction<JSONObject, List<JSONObject>, TimeWindow> {
    @Override
    public void apply(TimeWindow window, Iterable<JSONObject> values, Collector<List<JSONObject>> out) throws Exception {
        final HashMap<String, JSONObject> map = new HashMap<>();
        for (JSONObject jsonObject : values) {   // 遍历窗口中所有集卡，取最新集卡数据
            final String deviceNo = jsonObject.getString("deviceNo");
            final JSONObject latestJSON = map.get(deviceNo);
            if (latestJSON == null) {
                map.put(deviceNo, jsonObject);
                continue;
            }
            Long latestTime = latestJSON.getLong("eventTime");
            Long latestOffset = latestJSON.getLong("offset");
            Long eventTime = jsonObject.getLong("eventTime");
            Long offset = jsonObject.getLong("offset");
            // 取eventTime 最大的集卡信息，eventTime相同时取offset较大的
            if (eventTime > latestTime || (eventTime.equals(latestTime) && offset > latestOffset)) {
                map.put(deviceNo, jsonObject);
            }
        }
        if (map.size() > 0) {
            final String stat_time = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(window.getEnd());
            for (JSONObject value : map.values()) {
                value.put("stat_time", stat_time);
            }
            out.collect(new ArrayList<>(map.values()));
        }

    }
}
