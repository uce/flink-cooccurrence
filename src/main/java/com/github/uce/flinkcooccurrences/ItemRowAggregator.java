package com.github.uce.flinkcooccurrences;

import it.unimi.dsi.fastutil.ints.Int2ShortOpenHashMap;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

class ItemRowAggregator {

  private ItemRowAggregator() {
  }

  static class ItemCooccurrenceRowAggregateFunction
      implements AggregateFunction<ItemCooccurrences, Int2ShortOpenHashMap, Int2ShortOpenHashMap> {

    private static final long serialVersionUID = -7322064641409551098L;

    @Override
    public Int2ShortOpenHashMap createAccumulator() {
      return new Int2ShortOpenHashMap(8);
    }

    @Override
    public void add(ItemCooccurrences itemCooccurrences, Int2ShortOpenHashMap acc) {
      short increment = itemCooccurrences.getIncrement();
      for (int otherItem : itemCooccurrences) {
        acc.addTo(otherItem, increment);
      }
    }

    @Override
    public Int2ShortOpenHashMap getResult(Int2ShortOpenHashMap acc) {
      return acc;
    }

    @Override
    public Int2ShortOpenHashMap merge(Int2ShortOpenHashMap a, Int2ShortOpenHashMap b) {
      throw new UnsupportedOperationException("Flink 1.3.2 does not merge");
    }
  }

  static class ItemCooccurrenceRowWindowFunction
      extends ProcessWindowFunction<Int2ShortOpenHashMap, Tuple2<Integer, Int2ShortOpenHashMap>, Integer, TimeWindow> {

    private static final long serialVersionUID = -8843478976527903929L;

    @Override
    public void process(
        Integer item,
        Context ctx,
        Iterable<Int2ShortOpenHashMap> accus,
        Collector<Tuple2<Integer, Int2ShortOpenHashMap>> out) {
      out.collect(new Tuple2<>(item, accus.iterator().next()));
    }
  }

}
