package com.ds.flink.core.opeator.topN;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @ClassName TopNHotItems
 * @Description 求某个窗口中前N名的热门点击商品，key为窗口时间戳，输出为Top N 的结果字符串
 * @Author ds-longju
 * @Date 2022/8/3 3:24 下午
 * @Version 1.0
 **/
public class TopNHotItems  extends KeyedProcessFunction<Tuple, ItemBuyCount, List<ItemBuyCount>> {

    private final int topSize;

    public TopNHotItems(int topSize) {
        this.topSize = topSize;
    }

    //用于存储商品与购买数的状态，待收齐同一个窗口的数据后，再触发 Top N 计算
    private ListState<ItemBuyCount> itemState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //状态注册
        ListStateDescriptor<ItemBuyCount> itemViewStateDesc = new ListStateDescriptor<ItemBuyCount>(
                "itemState-state", ItemBuyCount.class
        );
        itemState = getRuntimeContext().getListState(itemViewStateDesc);
    }

    @Override
    public void processElement(ItemBuyCount input, KeyedProcessFunction<Tuple, ItemBuyCount, List<ItemBuyCount>>.Context context, Collector<List<ItemBuyCount>> collector) throws Exception {
        //每条数据都保存到状态
        itemState.add(input);
        //注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收集好了所有 windowEnd的商品数据
        context.timerService().registerEventTimeTimer(input.windowEnd + 1);

    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Tuple, ItemBuyCount, List<ItemBuyCount>>.OnTimerContext ctx, Collector<List<ItemBuyCount>> out) throws Exception {
        //获取收集到的所有商品点击量
        List<ItemBuyCount> allItems = new ArrayList<ItemBuyCount>();
        for(ItemBuyCount item : itemState.get()) {
            allItems.add(item);
        }
        //提前清除状态中的数据，释放空间
        itemState.clear();
        //按照点击量从大到小排序
        allItems.sort(new Comparator<ItemBuyCount>() {
            @Override
            public int compare(ItemBuyCount o1, ItemBuyCount o2) {
                return (int) (o2.buyCount - o1.buyCount);
            }
        });

        List<ItemBuyCount> itemBuyCounts = new ArrayList<>();
        //将排名信息格式化成String，方便打印
        StringBuilder result = new StringBuilder();
        result.append("========================================\n");
        result.append("时间：").append(new Timestamp(timestamp-1)).append("\n");
        for (int i=0;i<topSize;i++) {
            ItemBuyCount currentItem = allItems.get(i);
            // No1:  商品ID=12224  购买量=2
            result.append("No").append(i).append(":")
                    .append("  商品ID=").append(currentItem.itemId)
                    .append("  购买量=").append(currentItem.buyCount)
                    .append("\n");
            itemBuyCounts.add(currentItem);
        }
        result.append("====================================\n\n");

        out.collect(itemBuyCounts);

    }
}
