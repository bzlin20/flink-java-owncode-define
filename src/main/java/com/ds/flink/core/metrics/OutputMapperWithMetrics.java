package com.ds.flink.core.metrics;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;

/**
 * @ClassName:
 * @Description:
 * @author: ds-longju
 * @Date: 2022-09-01 15:45
 * @Version 1.0
 **/

public class OutputMapperWithMetrics  extends RichMapFunction<String, String> {
    public transient Counter outRecords;
    public transient Counter outDirtyRecords;
    public transient Meter outRecordsRate;

    @Override
    public void open(Configuration config) {
        outRecords = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_OUT);

        outDirtyRecords = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_DIRTY_RECORDS_OUT);

        outRecordsRate = getRuntimeContext().getMetricGroup().meter(MetricConstant.DT_NUM_RECORDS_OUT_RATE, new MeterView(outRecords, 20));
    }

    @Override
    public String map(String value) throws Exception {
        try {
            // 写出成功
            outRecords.inc();
            return value;
        } catch (Exception e) {
            // 写出失败
            outDirtyRecords.inc();
            return value;

        }
    }

}
