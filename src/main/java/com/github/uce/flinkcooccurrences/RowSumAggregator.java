package com.github.uce.flinkcooccurrences;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;

class RowSumAggregator {

  static class RowSumAggregateFunction implements AggregateFunction<Tuple2<Integer, Integer>, IntValue, Integer> {

    private static final long serialVersionUID = -5083920710345287266L;

    @Override
    public IntValue createAccumulator() {
      return new IntValue();
    }

    @Override
    public void add(Tuple2<Integer, Integer> value, IntValue acc) {
      acc.setValue(acc.getValue() + value.f1);
    }

    @Override
    public Integer getResult(IntValue acc) {
      return acc.getValue();
    }

    @Override
    public IntValue merge(IntValue a, IntValue b) {
      throw new UnsupportedOperationException("Flink 1.3.2 does not merge");
    }
  }

  static class RowSumProcessWindow extends ProcessWindowFunction<Integer, Tuple2<Integer, Integer>, Tuple, TimeWindow> {

    private static final long serialVersionUID = 6411660327603408586L;

    private final Tuple2<Integer, Integer> reuse = new Tuple2<>();

    private LongCounter rowSums;

    @Override
    public void open(Configuration parameters) {
      rowSums = getRuntimeContext().getLongCounter("RowSumProcessWindowRowSum");
    }

    @Override
    public void process(
        Tuple key,
        Context context,
        Iterable<Integer> elements,
        Collector<Tuple2<Integer, Integer>> out) {

      int item = key.getField(0);
      int rowSumUpdate = 0;
      for (int update : elements) {
        rowSumUpdate += update;
      }

      if (rowSumUpdate != 0) {
        rowSums.add(rowSumUpdate);
        reuse.setFields(item, rowSumUpdate);
        out.collect(reuse);
      }
    }
  }

}
