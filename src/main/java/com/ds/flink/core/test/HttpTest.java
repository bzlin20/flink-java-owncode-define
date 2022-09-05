package com.ds.flink.core.test;



import com.alibaba.fastjson.JSONObject;
import com.ds.flink.core.until.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * @ClassName HttpTest
 * @Description 请描述类的业务用途
 * @Author ds-longju
 * @Date 2022/7/25 9:59 上午
 * @Version 1.0
 **/


public class HttpTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtils.class);
    private String url;


    private static String httpGet(String getUrl) throws Exception {
        URL url = new URL(getUrl);
        // 打开和URL之间的连接
        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setRequestMethod("POST");//请求post方式
        con.setUseCaches(false); // Post请求不能使用缓存
        con.setDoInput(true);// 设置是否从HttpURLConnection输入，默认值为 true
        con.setDoOutput(true);// 设置是否使用HttpURLConnection进行输出，默认值为 false

        //设置header内的参数 connection.setRequestProperty("健, "值");
        con.setRequestProperty("Content-Type", "application/json");

        //设置body内的参数，put到JSONObject中
        JSONObject param = new JSONObject();
        param.put("clientKey", "guomao");
        param.put("userId", "16888");
        param.put("passWord", "Z21xaDc3ODg5OQ==");

        // 建立实际的连接
        con.connect();

        // 得到请求的输出流对象
        OutputStreamWriter writer = new OutputStreamWriter(con.getOutputStream(),"UTF-8");
        writer.write(param.toString());
        writer.flush();

        // 获取服务端响应，通过输入流来读取URL的响应
        InputStream is = con.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        StringBuffer sbf = new StringBuffer();
        String strRead = null;
        while ((strRead = reader.readLine()) != null) {
            sbf.append(strRead);
            sbf.append("\r\n");
        }
        reader.close();

        // 关闭连接
        con.disconnect();

        // 返回运行结果
        return sbf.toString();

    }
    public static void main(String[] args) throws Exception {
        String s = httpGet("http://172.16.89.108:8080/trader/v1.0/getReqQryDepthMarketData");
        System.out.println(s);

    }
}
