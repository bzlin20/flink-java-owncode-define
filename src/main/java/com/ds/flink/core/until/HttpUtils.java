package com.ds.flink.core.until;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @ClassName HttpUtils
 * @Description http请求工具类
 * @Author ds-longju
 * @Date 2022/7/24 11:45 上午
 * @Version 1.0
 * @use env.addSource(new HttpUtils ( " 你的url ")).print();
 **/
public class HttpUtils extends RichSourceFunction<String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtils .class);
    private String url;
    private static HttpURLConnection con = null;
    private static BufferedReader in = null;

    public HttpUtils(String url) throws UnsupportedEncodingException {
        this.url = url;
        /*
           TODO　１.添加请求参数
           如果有一些查询参数，请求体参数直接拼接在url后边，拼接时注意使用指定字符格式（URLEncoder.encode），避免空格
                url = "https://open.feishu.cn/open-apis/bitable/v1/apps/"
                    + URLEncoder.encode("APP_TOKEN", "utf-8")
                    +"/tables/"+URLEncoder.encode("TABLE_ID", "utf-8")
                    +"/records?view_id="+URLEncoder.encode("VIEW_ID", "utf-8")
                    + ("HTTP_FILTER" != null ? "&filter=" + URLEncoder.encode("HTTP_FILTER", "utf-8") : "");
         */
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        sourceContext.collect(httpGet(url));
    }

    @Override
    public void cancel() {

    }
    private static String httpGet(String getUrl) {

        StringBuilder inputString = new StringBuilder();

        try {
            URL url = new URL(getUrl);
            con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");//设置请求模式
            //TODO 3.设置请求头参数（返回类型、字符集、token）
            con.setRequestProperty("Content-Type", "application/json");//设置请求头参数
//            con.setRequestProperty("charset", "utf-8");
//            con.setRequestProperty("Authorization", "Bearer YOUR_ACCESS_TOKEN");
            con.setRequestProperty("clientKey","guomao");
            con.setRequestProperty("userId","16888");
            con.setRequestProperty("passWord","Z21xaDc3ODg5OQ==");
            in = new BufferedReader(new InputStreamReader(con.getInputStream()));//字符流获取数据

            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                inputString.append(inputLine);
            }
        } catch (Exception var16) {
            LOGGER.warn("httpget threw: ", var16);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
                if (con != null) {
                    con.disconnect();
                }
            } catch (Exception var15) {
                LOGGER.warn("httpget finally block threw: ", var15);
            }
        }
        return inputString.toString();
    }

}
