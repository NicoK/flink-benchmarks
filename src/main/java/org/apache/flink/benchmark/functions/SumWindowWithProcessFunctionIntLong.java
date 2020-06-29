package org.apache.flink.benchmark.functions;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.ververica.windowing.ProcessWindowFunction;

class SumWindowWithProcessFunctionIntLong implements
        ProcessWindowFunction<IntegerLongSource.Record, IntegerLongSource.Record, Integer> {
    @Override
    public void process(Integer key, TimeWindow timeWindow,
                        Iterable<IntegerLongSource.Record> input,
                        Collector<IntegerLongSource.Record> out)
            throws Exception {
        long sum = 0L;
        for (IntegerLongSource.Record element : input) {
            sum += element.value;
        }
        out.collect(IntegerLongSource.Record.of(key, sum));
    }
}
