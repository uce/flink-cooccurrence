package com.github.uce.flinkcooccurrences;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.io.IOException;
import java.util.Collection;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.io.BlockingQueueBroker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class UserInteractionCounterOneInputStreamOperator extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<Tuple3<Integer, Integer, Boolean>, Void>,
    Triggerable<Tuple, TimeWindow> {

  private static final long serialVersionUID = 7591214393630130526L;

  private static final Logger LOG = LoggerFactory.getLogger(UserInteractionCounterOneInputStreamOperator.class);

  @SuppressWarnings("serial")
  private static final OutputTag<ItemCooccurrences> ITEM_TAG =
      new OutputTag<ItemCooccurrences>("itemCooccurrences") {};

  @SuppressWarnings("serial")
  private static final OutputTag<Tuple2<Integer, Integer>> ROW_SUM_TAG =
      new OutputTag<Tuple2<Integer, Integer>>("rowSums") {};

  private final UUID feedbackId;
  private final short userCut;
  private final Random random;

  private transient BlockingQueue<Object> feedbackQueue;

  private final TumblingEventTimeWindows windowAssigner;
  private final TypeSerializer<TimeWindow> windowSerializer;

  private transient Tuple2<Integer, Integer> rowSumsReuse;
  private transient StreamRecord<Tuple2<Integer, Integer>> rowSumsOutputRecord;
  private transient ItemCooccurrences itemCooccurrencesReuse;
  private transient StreamRecord<ItemCooccurrences> itemCooccurrencesOutputRecord;
  private transient InternalTimerService<TimeWindow> timerService;
  private transient InternalListState<TimeWindow, Tuple3<Integer, Integer, Boolean>> windowState;
  private transient ValueState<Short> userInteractionsState;
  private transient ValueState<Integer> userInteractionsTotalState;
  private transient ValueState<IntArrayList> userHistoryState;
  private transient IntCounter lateElements;
  private transient LongCounter observedCooccurrences;

  UserInteractionCounterOneInputStreamOperator(
      UUID feedbackId,
      short userCut,
      long seed,
      int windowSize,
      TimeUnit windowUnit) {
    this.feedbackId = feedbackId;
    this.userCut = userCut;
    this.random = new Random(seed);
    this.windowAssigner = TumblingEventTimeWindows.of(Time.of(windowSize, windowUnit));
    this.windowSerializer = windowAssigner.getWindowSerializer(null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void open() throws Exception {
    this.rowSumsReuse = new Tuple2<>();
    this.rowSumsOutputRecord = new StreamRecord<>(rowSumsReuse);
    this.itemCooccurrencesReuse = new ItemCooccurrences();
    this.itemCooccurrencesOutputRecord = new StreamRecord<>(itemCooccurrencesReuse);
    this.timerService = getInternalTimerService("window-timers", windowSerializer, this);
    this.windowState = (InternalListState<TimeWindow, Tuple3<Integer, Integer, Boolean>>)
        getOrCreateKeyedState(windowSerializer, new ListStateDescriptor<>(
            "windowState", new TypeHint<Tuple3<Integer, Integer, Boolean>>() {}.getTypeInfo()));
    userInteractionsState = getKeyedStateStore().getState(new ValueStateDescriptor<>(
        "userInteractions", Short.class));
    userInteractionsTotalState = getKeyedStateStore().getState(new ValueStateDescriptor<>(
        "userInteractionsTotal", Integer.class));
    userHistoryState = getKeyedStateStore().getState(new ValueStateDescriptor<>(
        "userHistory", new TypeHint<IntArrayList>() {}.getTypeInfo()));

    String brokerString = FeedbackSource.getBrokerString(feedbackId, getRuntimeContext().getIndexOfThisSubtask());
    LOG.info("Requesting access to feedback queue via broker ({})", brokerString);
    feedbackQueue = (BlockingQueue<Object>) BlockingQueueBroker.INSTANCE.getAndRemove(brokerString);
    LOG.info("Received feedback queue {}", feedbackQueue.hashCode());
    getRuntimeContext().getIntCounter("UserInteractionCounterFeedbackQueues").add(1);

    this.lateElements = getRuntimeContext().getIntCounter("UserInteractionCounterLateElements");
    this.observedCooccurrences = getRuntimeContext().getLongCounter("UserInteractionCounterObservedCooccurrences");
  }

  @Override
  public void processElement(StreamRecord<Tuple3<Integer, Integer, Boolean>> element) throws Exception {
    final Tuple3<Integer, Integer, Boolean> interaction = element.getValue();
    final long timestamp = element.getTimestamp();
    final long watermark = timerService.currentWatermark();

    if (timestamp <= watermark) {
      this.lateElements.add(1);
      LOG.info("Ignoring late interaction {} (timestamp: {}, watermark: {})", interaction, timestamp, watermark);
    } else {
      Collection<TimeWindow> assignedWindows = windowAssigner.assignWindows(null, timestamp, null);
      if (assignedWindows.size() > 1) {
        throw new IllegalStateException("Tumbling window but assigned interaction to multiple windows");
      }

      final TimeWindow window = assignedWindows.iterator().next();
      windowState.setCurrentNamespace(window);
      windowState.add(interaction);

      if (FlinkCooccurrences.DEVELOPMENT_MODE) {
        getRuntimeContext().getIntCounter("UserInteractionCounterReceivedElements").add(1);
        getRuntimeContext().getIntCounter("UserInteractionCounterBufferedElements").add(1);
      }

      // Actual registration is only done once per (key,namespace,timestamp)
      timerService.registerEventTimeTimer(window, window.maxTimestamp());
    }
  }

  @Override
  public void onEventTime(InternalTimer<Tuple, TimeWindow> timer) throws Exception {
    TimeWindow window = timer.getNamespace();
    long timestamp = window.maxTimestamp();
    windowState.setCurrentNamespace(window);

    int userInteractionsTotal = getOrDefault(userInteractionsTotalState, 0);
    short userInteractions = getOrDefault(userInteractionsState, (short) 0);

    for (Tuple3<Integer, Integer, Boolean> interaction : windowState.get()) {
      if (FlinkCooccurrences.DEVELOPMENT_MODE) {
        getRuntimeContext().getIntCounter("UserInteractionCounterBufferedElements").add(-1);
      }

      userInteractionsTotal++;

      int item = interaction.f1;
      boolean sample = interaction.f2;

      // Update timestamp for all outgoing elements
      rowSumsOutputRecord.setTimestamp(timestamp);
      itemCooccurrencesOutputRecord.setTimestamp(timestamp);

      if (sample) {
        if (userInteractions < userCut) {
          userInteractions++;

          IntArrayList userHistory = userHistoryState.value();
          if (userHistory == null) {
            userHistory = new IntArrayList();
          }

          int[] otherItems = userHistory.elements();
          int size = userHistory.size();

          if (size > 0) {
            itemCooccurrencesReuse.setFields(item, otherItems, size, (short) 1);
            output.collect(ITEM_TAG, itemCooccurrencesOutputRecord);

            rowSumsReuse.setFields(item, size);
            output.collect(ROW_SUM_TAG, rowSumsOutputRecord);

            for (int i = 0; i < size; i++) {
              int otherItem = otherItems[i];
              itemCooccurrencesReuse.setFields(otherItem, item, (short) 1);
              output.collect(ITEM_TAG, itemCooccurrencesOutputRecord);

              rowSumsReuse.setFields(otherItem, 1);
              output.collect(ROW_SUM_TAG, rowSumsOutputRecord);
            }

            observedCooccurrences.add(2 * size);

            if (FlinkCooccurrences.DEVELOPMENT_MODE) {
              getRuntimeContext().getIntCounter("UserInteractionCounterRowSums").add(2 * size);
            }
          }

          userHistory.add(item);
          userHistoryState.update(userHistory);

          userInteractionsState.update(userInteractions);
        } else {
          int k = random.nextInt(userInteractionsTotal);

          if (k < userCut) {
            IntArrayList userHistory = userHistoryState.value();
            int previousItem = userHistory.getInt(k);

            int[] otherItems = userHistory.elements();

            itemCooccurrencesReuse.setFields(item, otherItems, userCut, k, (short) 1);
            output.collect(ITEM_TAG, itemCooccurrencesOutputRecord);

            rowSumsReuse.setFields(item, userCut - 1);
            output.collect(ROW_SUM_TAG, rowSumsOutputRecord);

            itemCooccurrencesReuse.setFields(previousItem, otherItems, userCut, k, (short) -1);
            output.collect(ITEM_TAG, itemCooccurrencesOutputRecord);

            rowSumsReuse.setFields(previousItem, -(userCut - 1));
            output.collect(ROW_SUM_TAG, rowSumsOutputRecord);

            if (FlinkCooccurrences.DEVELOPMENT_MODE) {
              getRuntimeContext().getIntCounter("UserInteractionCounterRowSums").add(userCut - 1);
              getRuntimeContext().getIntCounter("UserInteractionCounterRowSums").add(-(userCut - 1));
            }

            for (int i = 0; i < userCut; i++) {
              if (i != k) {
                int otherItem = otherItems[i];

                // No row sum update needed, because +1 + -1 = 0 :-)
                itemCooccurrencesReuse.setFields(otherItem, item, (short) 1);
                output.collect(ITEM_TAG, itemCooccurrencesOutputRecord);

                itemCooccurrencesReuse.setFields(otherItem, previousItem, (short) -1);
                output.collect(ITEM_TAG, itemCooccurrencesOutputRecord);
              }
            }
            userHistory.set(k, item);
            userHistoryState.update(userHistory);
          } else {
            feedbackQueue.add(new Tuple2<>(item, -1));
          }
        }
      }
    }

    userInteractionsTotalState.update(userInteractionsTotal);
    userInteractionsState.update(userInteractions);

    windowState.clear();
  }

  @Override
  public void onProcessingTime(InternalTimer<Tuple, TimeWindow> timer) throws Exception {
    throw new IllegalStateException("How dare you use processing time?");
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    super.processWatermark(mark);

    // Terminate the feedback source...
    if (mark.getTimestamp() == Long.MAX_VALUE) {
      feedbackQueue.put(FeedbackSource.EndOfFeedback.Instance);
    }
  }

  // -------------------------------------------------------------------------------------------------------------------

  static TypeInformation<Void> getOutputType() {
    return new TypeHint<Void>() {}.getTypeInfo();
  }

  static OutputTag<ItemCooccurrences> getItemCooccurrencesTag() {
    return ITEM_TAG;
  }

  static OutputTag<Tuple2<Integer, Integer>> getRowSumTag() {
    return ROW_SUM_TAG;
  }

  private static <T> T getOrDefault(ValueState<T> valueState, T defaultValue) throws IOException {
    T value = valueState.value();
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }
}
