package com.ds.flink.core.transformation;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;

/**
 * @ClassName HttpAsyncFunction
 * @Description 请描述类的业务用途
 * @Author ds-longju
 * @Date 2022/7/19 11:54 上午
 * @Version 1.0
 **/
public class HttpAsyncFunction{
//public class HttpAsyncFunction extends RichAsyncFunction<String, String> {
//    private static final long serialVersionUID = 8522411971886428444L;
//
//    private static final long TERMINATION_TIMEOUT = 5000L;
//    private static final int THREAD_POOL_SIZE = 10;
//    private String code;
//    static ExecutorService executorService;
//    static int counter = 0;
//    private static HashMap<String, String> configMap = GlobalConfigUtil.configMap;
//
//    public HttpAsyncFunction(String code) {
//        this.code = code;
//    }
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        synchronized (HttpAsyncFunction.class) {
//            if (counter == 0) {
//                executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
//            }
//
//            ++counter;
//        }
//    }
//
//    private void freeExecutor() {
//        synchronized (HttpAsyncFunction.class) {
//            --counter;
//
//            if (counter == 0) {
//                executorService.shutdown();
//
//                try {
//                    if (!executorService.awaitTermination(TERMINATION_TIMEOUT, TimeUnit.MILLISECONDS)) {
//                        executorService.shutdownNow();
//                    }
//                } catch (InterruptedException interrupted) {
//                    executorService.shutdownNow();
//
//                    Thread.currentThread().interrupt();
//                }
//            }
//        }
//    }
//
//    @Override
//    public void close() throws Exception {
//        super.close();
//        freeExecutor();
//    }
//
//    @Override
//    public void asyncInvoke(String json, ResultFuture<String> resultFuture) throws Exception {
//        executorService.submit(new Runnable() {
//            @Override
//            public void run() {
//                String result = msgPush(json);
//                resultFuture.complete(Collections.singleton(result));
//            }
//        });
//    }
//    //接口的调用,对于里面的数据解析,根据自己的业务需求编码,工具类参考博主上篇博客
//    public String msgPush(String json) {
//        JSONObject rootObject = JSON.parseObject(json);
//        String requestData = rootObject.get("json").toString();
//        String code = rootObject.get("code").toString();
//        String phoneNum = rootObject.get("phoneNum").toString();
//        String province = rootObject.get("province").toString();
//
//        JSONObject jsonObject = new JSONObject();
//        jsonObject.put("interfaceSign",code);
//        jsonObject.put("phoneNum",phoneNum);
//        jsonObject.put("province",province);
//        jsonObject.put("responseTime",System.currentTimeMillis());
//        //调用接口
//        String url = "你自己的url";
//        String responseBody = DoPostUtil.doPost(requestData, url);
//        //数据的封装
//        try {
//            JSONObject responseJSON = JSON.parseObject(responseBody);
//            jsonObject.put("errMessage","成功");
//            jsonObject.put("responseBody",responseJSON);
//            return jsonObject.toString();
//        }catch (Exception e){
//            jsonObject.put("errMessage","成功");
//            jsonObject.put("responseBody","null");
//            return jsonObject.toString();
//        }
//    }
}
