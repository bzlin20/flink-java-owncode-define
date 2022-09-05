package com.ds.flink.core.faulttolerance.Correctness.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @ClassName: Tuple3KeySelector
 * @Description: 指定key
 * @author: ds-longju
 * @Date: 2022-09-05 14:11
 * @Version 1.0
 **/
public class Tuple3KeySelector implements KeySelector<Tuple3<String, Long, Long>, String> {
    @Override
    public String getKey(Tuple3<String, Long, Long> stringLongLongTuple3) throws Exception {
        return stringLongLongTuple3.f0;
    }
}
