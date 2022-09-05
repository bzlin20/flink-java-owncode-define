package com.ds.flink.core.opeator.topN;

/**
 * @ClassName UserAction
 * @Description  商品类
 * @Author ds-longju
 * @Date 2022/8/3 2:23 下午
 * @Version 1.0
 **/
public class UserAction {
    public long userId; //用户id
    public long itemId; //商品id
    public int categoryId; //商品分类id
    public String behavior; //用户行为（pv, buy, cart, fav)
    public long timestamp; //操作时间戳

    public long getUserId() {
        return userId;
    }

    public long getItemId() {
        return itemId;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
