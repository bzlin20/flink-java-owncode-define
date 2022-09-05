package com.ds.flink.core.opeator.topN;

/**
 * @ClassName ItemBuyCount
 * @Description 商品购买量（窗口操作的输出类型）
 * @Author ds-longju
 * @Date 2022/8/3 3:20 下午
 * @Version 1.0
 **/
public class ItemBuyCount {

    public long itemId; //商品ID;
    public long windowEnd; //窗口结束时间戳
    public long buyCount; //购买数量

    public static ItemBuyCount of(long itemId, long windowEnd, long buyCount) {
        ItemBuyCount itemBuyCount = new ItemBuyCount();
        itemBuyCount.itemId = itemId;
        itemBuyCount.windowEnd = windowEnd;
        itemBuyCount.buyCount = buyCount;
        return itemBuyCount;
    }

    @Override
    public String toString() {
        return "ItemBuyCount{" +
                "itemId=" + itemId +
                ", windowEnd=" + windowEnd +
                ", buyCount=" + buyCount +
                '}';
    }
}
