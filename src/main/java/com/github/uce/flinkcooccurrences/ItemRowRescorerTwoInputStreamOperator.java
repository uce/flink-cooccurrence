package com.github.uce.flinkcooccurrences;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ShortMap;
import it.unimi.dsi.fastutil.ints.Int2ShortOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

class ItemRowRescorerTwoInputStreamOperator extends AbstractStreamOperator<Tuple2<Integer, IntDoublePriorityQueue>>
    implements
    TwoInputStreamOperator<
        Tuple2<Integer, Int2ShortOpenHashMap>,
        Tuple2<Integer, Integer>,
        Tuple2<Integer, IntDoublePriorityQueue>> {

  private static final long serialVersionUID = -4774211415932141560L;

  private final short topK;

  private Int2IntOpenHashMap globalItemRowSums;

  private Map<Integer, Int2ShortOpenHashMap> itemRows;

  private long observedCooccurrences;

  private TreeMap<Long, List<Tuple2<Integer, Integer>>> bufferedRowSumUpdates;

  private TreeMap<Long, Map<Integer, Int2ShortOpenHashMap>> bufferedItemRowDeltas;

  private Tuple2<Integer, IntDoublePriorityQueue> itemTopKReuse;

  private IntDoublePriorityQueue topKReuse;

  private StreamRecord<Tuple2<Integer, IntDoublePriorityQueue>> outputRecordReuse;

  private LongCounter rescoredItems;

  ItemRowRescorerTwoInputStreamOperator(short topK) {
    if (topK <= 0) {
      throw new IllegalArgumentException(topK + " is <= 0");
    }
    this.topK = topK;
  }

  @Override
  public void open() {
    this.rescoredItems = getRuntimeContext().getLongCounter("ItemRowRescorerRescoredItems");
    this.globalItemRowSums = new Int2IntOpenHashMap();
    this.itemRows = new HashMap<>();
    this.bufferedRowSumUpdates = new TreeMap<>();
    this.bufferedItemRowDeltas = new TreeMap<>();

    this.topKReuse = new IntDoublePriorityQueue(topK);
    this.itemTopKReuse = new Tuple2<>();
    this.outputRecordReuse = new StreamRecord<>(itemTopKReuse);
  }

  @Override
  public void close() {
    if (!bufferedRowSumUpdates.isEmpty()) {
      throw new IllegalStateException("Buffered row sums not empty: " + bufferedRowSumUpdates);
    }

    if (!bufferedItemRowDeltas.isEmpty()) {
      throw new IllegalStateException("Buffered item rows not empty: " + bufferedRowSumUpdates);
    }
  }

  @Override
  public void processElement1(StreamRecord<Tuple2<Integer, Int2ShortOpenHashMap>> element) {
    long timestamp = element.getTimestamp();
    Tuple2<Integer, Int2ShortOpenHashMap> itemRow = element.getValue();

    Map<Integer, Int2ShortOpenHashMap> buffered = bufferedItemRowDeltas.computeIfAbsent(
        timestamp,
        (ignored) -> new HashMap<>());

    if (buffered.put(itemRow.f0, itemRow.f1) != null) {
      throw new IllegalStateException("Received duplicate item row for item " + itemRow.f0 + " at " + timestamp);
    }

    if (FlinkCooccurrences.DEVELOPMENT_MODE) {
      getRuntimeContext().getIntCounter("ItemRowRescorerBufferedItemRows").add(1);
    }
  }

  @Override
  public void processElement2(StreamRecord<Tuple2<Integer, Integer>> element) {
    long timestamp = element.getTimestamp();

    List<Tuple2<Integer, Integer>> buffered = bufferedRowSumUpdates.computeIfAbsent(
        timestamp,
        (ignored) -> new ArrayList<>());

    buffered.add(new Tuple2<>(element.getValue().f0, element.getValue().f1));

    if (FlinkCooccurrences.DEVELOPMENT_MODE) {
      getRuntimeContext().getIntCounter("ItemRowRescorerBufferedRowSumUpdates").add(1);
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    if (!bufferedRowSumUpdates.isEmpty()) {
      long watermark = mark.getTimestamp();

      Iterator<Map.Entry<Long, List<Tuple2<Integer, Integer>>>> iterator = bufferedRowSumUpdates.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Long, List<Tuple2<Integer, Integer>>> entry = iterator.next();
        long timestamp = entry.getKey();

        if (timestamp <= watermark) {
          updateGlobalItemRowSums(entry.getValue());
          iterator.remove();

          Map<Integer, Int2ShortOpenHashMap> itemRows = bufferedItemRowDeltas.remove(timestamp);
          if (itemRows != null) {
            // This can be null if we did not get any item for this window
            scoreItemRows(itemRows, watermark);
            itemRows.clear();
          }
        } else {
          break;
        }
      }
    }

    super.processWatermark(mark);
  }

  private void updateGlobalItemRowSums(List<Tuple2<Integer, Integer>> itemRowSumUpdates) {
    for (Tuple2<Integer, Integer> itemRowSumUpdate : itemRowSumUpdates) {
      if (FlinkCooccurrences.DEVELOPMENT_MODE) {
        getRuntimeContext().getIntCounter("ItemRowRescorerBufferedRowSumUpdates").add(-1);
      }

      int item = itemRowSumUpdate.f0;
      int rowSumUpdate = itemRowSumUpdate.f1;

      globalItemRowSums.addTo(item, rowSumUpdate);
      observedCooccurrences += rowSumUpdate;
    }
  }

  private void scoreItemRows(Map<Integer, Int2ShortOpenHashMap> itemRowDeltas, long timestamp) {
    outputRecordReuse.setTimestamp(timestamp);

    for (Map.Entry<Integer, Int2ShortOpenHashMap> itemDelta : itemRowDeltas.entrySet()) {
      if (FlinkCooccurrences.DEVELOPMENT_MODE) {
        getRuntimeContext().getIntCounter("ItemRowRescorerBufferedItemRows").add(-1);
      }

      int item = itemDelta.getKey();
      Int2ShortOpenHashMap delta = itemDelta.getValue();

      rescoredItems.add(1);

      // Update aggregate first...
      Int2ShortOpenHashMap itemRow = itemRows.computeIfAbsent(item, (ignored) -> new Int2ShortOpenHashMap());
      ObjectIterator<Int2ShortMap.Entry> deltaIterator = delta.int2ShortEntrySet().fastIterator();
      while (deltaIterator.hasNext()) {
        Int2ShortMap.Entry rowDelta = deltaIterator.next();
        itemRow.addTo(rowDelta.getIntKey(), rowDelta.getShortValue());
      }

      delta.clear();

      int itemRowSum = globalItemRowSums.get(item);

      if (FlinkCooccurrences.DEVELOPMENT_MODE) {
        int actualItemRowSum = 0;
        for (short itemCooccurrenceCount : itemRow.values()) {
          actualItemRowSum += itemCooccurrenceCount;
        }

        if (actualItemRowSum != itemRowSum) {
          throw new IllegalStateException("Item row " + itemRowSum
              + " does not match actual row sum " + actualItemRowSum);
        }
      }

      ObjectIterator<Int2ShortMap.Entry> itemRowIterator = itemRow.int2ShortEntrySet().fastIterator();

      topKReuse.reset();

      while (itemRowIterator.hasNext()) {
        Int2ShortMap.Entry itemCooccurrence = itemRowIterator.next();
        int otherItem = itemCooccurrence.getIntKey();

        short cooccurrenceCount = itemCooccurrence.getShortValue();
        long otherItemRowSum = globalItemRowSums.get(otherItem);
        double score = scoreItem(cooccurrenceCount, itemRowSum, otherItemRowSum, observedCooccurrences);

        if (FlinkCooccurrences.DEVELOPMENT_MODE) {
          if (Double.isNaN(score)) {
            throw new IllegalStateException("Score is NaN "
                + "(item: " + item + ", otherItem: " + otherItem + ", "
                + "cooccurrenceCount: " + cooccurrenceCount + ", "
                + "itemRowSum: " + itemRowSum + ", "
                + "otherItemRowSum: " + otherItemRowSum + ", "
                + "observedCooccurrences: " + observedCooccurrences + ")");
          }
        }

        if (topKReuse.getSize() < topK) {
          topKReuse.add(otherItem, score);
        } else if (score > topKReuse.getLeastScore()) {
          topKReuse.update(otherItem, score);
        }
      }

      itemTopKReuse.setFields(item, topKReuse);
      output.collect(outputRecordReuse);
    }
  }

  private static double scoreItem(
      short k11,
      long itemRowSum,
      long otherItemRowSum,
      long observedCooccurrences) {

    final long k12 = itemRowSum - k11;
    final long k21 = otherItemRowSum - k11;
    final long k22 = observedCooccurrences + k11 - k12 - k21;

    return LogLikelihood.logLikelihoodRatio(k11, k12, k21, k22);
  }

  static TypeInformation<Tuple2<Integer, IntDoublePriorityQueue>> getOutputType() {
    return new TypeHint<Tuple2<Integer, IntDoublePriorityQueue>>() {}.getTypeInfo();
  }
}
