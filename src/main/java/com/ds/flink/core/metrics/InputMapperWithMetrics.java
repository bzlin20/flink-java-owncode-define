package com.ds.flink.core.metrics;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;

/**
 * @ClassName: InputMapperWithMetrics
 * @Description: InputMapperWithMetrics
 * @author: ds-longju
 * @Date: 2022-09-01 15:44
 * @Version 1.0
 **/

public class InputMapperWithMetrics extends RichMapFunction<String, String> {
    /** tps transactions Per Second */
    protected transient Counter numInRecord;
    protected transient Meter numInRate;

    /** rps Record Per Second: deserialize data and out record num */
    protected transient Counter numInBytes;
    protected transient Meter numInBytesRate;

    // 输入曲线
    protected transient Counter numInResolveRecord;
    protected transient Meter numInResolveRate;

    //脏数据曲线
    protected transient Counter dirtyDataCounter;

    @Override
    public void open(Configuration config) {
        numInRecord = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_IN_COUNTER);

        numInRate = getRuntimeContext().getMetricGroup().meter(MetricConstant.DT_NUM_RECORDS_IN_RATE, new MeterView(numInRecord, 20));

        numInBytes = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_BYTES_IN_COUNTER);

        numInBytesRate = getRuntimeContext().getMetricGroup().meter(MetricConstant.DT_NUM_BYTES_IN_RATE, new MeterView(numInBytes, 20));

        numInResolveRecord = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_RESOVED_IN_COUNTER);
        numInResolveRate = getRuntimeContext().getMetricGroup().meter(MetricConstant.DT_NUM_RECORDS_RESOVED_IN_RATE, new MeterView(numInResolveRecord, 20));

        dirtyDataCounter = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_DIRTY_DATA_COUNTER);

    }


    @Override
    public String map(String value) throws Exception {
        // TPS 输入的事件/事务数
        numInRecord.inc();
        numInBytes.inc(value.getBytes().length);

        numInResolveRecord.inc();
        return value;
    }
}

