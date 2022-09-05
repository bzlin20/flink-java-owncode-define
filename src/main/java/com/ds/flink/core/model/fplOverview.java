package com.ds.flink.core.model;

/**
 * @ClassName fplOverview
 * @Description 敞口浮动盈亏_浮盈数据表
 * @Author ds-longju
 * @Date 2022/7/19 3:35 下午
 * @Version 1.0
 **/
public class fplOverview {
    public String id ;
    public String dp_id;
    public String datatime;
    public String fpl_amount;
    public String yw;

    @Override
    public String toString() {
        return "fplOverview{" +
                "id='" + id + '\'' +
                ", dp_id='" + dp_id + '\'' +
                ", datatime='" + datatime + '\'' +
                ", fpl_amount='" + fpl_amount + '\'' +
                ", yw='" + yw + '\'' +
                '}';
    }
    public String toJson(){
        return "{\"id\":\""+id + "\",\"dp_id\":\""+ dp_id + "\",\"datatime\":\""+datatime +"\",\"fpl_amount\":\""+fpl_amount +"\",\"yw\":\""+yw +"\"}";
    }

    public fplOverview(String id, String dp_id, String datatime, String fpl_amount, String yw) {
        this.id = id;
        this.dp_id = dp_id;
        this.datatime = datatime;
        this.fpl_amount = fpl_amount;
        this.yw = yw;
    }

    public String getId() {
        return id;
    }

    public String getDp_id() {
        return dp_id;
    }

    public String getDatatime() {
        return datatime;
    }

    public String getFpl_amount() {
        return fpl_amount;
    }

    public String getYw() {
        return yw;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setDp_id(String dp_id) {
        this.dp_id = dp_id;
    }

    public void setDatatime(String datatime) {
        this.datatime = datatime;
    }

    public void setFpl_amount(String fpl_amount) {
        this.fpl_amount = fpl_amount;
    }

    public void setYw(String yw) {
        this.yw = yw;
    }
}
