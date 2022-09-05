package com.ds.flink.core.source;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @ClassName HttpsTestSource
 * @Description 接入http接口数据
 * @Author ds-longju
 * @Date 2022/7/19 11:46 上午
 * @Version 1.0
 **/
public class HttpsTestSource  extends RichSourceFunction<String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpsTestSource.class);

    private static final long DEFAULT_SLEEP_MS = 2000;
    private  String url = "";
    private  long sleepMs = 1L;
    private volatile boolean isRunning;


    public HttpsTestSource(String url, long sleepMs) {
        this.isRunning = true;
        this.url = url;
        this.sleepMs = sleepMs;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (this.isRunning) {
            Thread.sleep(sleepMs);
            sourceContext.collect(httpGet(url));
        }

    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }

    private static String httpGet(String getUrl) throws Exception {
        HttpURLConnection con = null;
        BufferedReader in = null;
        StringBuilder inputString = new StringBuilder();
        try {
            URL url = new URL(getUrl);
            LOGGER.info("Trying to get now");
            con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");

            in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            double inputSize = 0;
            while ((inputLine = in.readLine()) != null) {
                inputString.append(inputLine);
                inputSize += inputLine.length();
                LOGGER.debug("read inputline - " + inputLine);
            }

        } catch (Exception e) {
            LOGGER.warn("httpget threw: ", e);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
                if (con != null) {
                    con.disconnect();
                }
            } catch (Exception e) {
                LOGGER.warn("httpget finally block threw: ", e);
            }
        }
        return inputString.toString();
    }


}
