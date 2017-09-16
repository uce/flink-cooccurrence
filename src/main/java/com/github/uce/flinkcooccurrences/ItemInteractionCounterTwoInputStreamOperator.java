package com.github.uce.flinkcooccurrences;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.accumulators.IntCounter;
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
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

class ItemInteractionCounterTwoInputStreamOperator extends AbstractStreamOperator<Tuple3<Integer, Integer, Boolean>>
    implements
    TwoInputStreamOperator<
        Tuple3<Integer, Integer, Long>,
        Tuple2<Integer, Integer>,
        Tuple3<Integer, Integer, Boolean>>,
    Triggerable<Tuple, TimeWindow> {

  private static final long serialVersionUID = -4774211415932141560L;

  private final short itemCut;

  private final TumblingEventTimeWindows windowAssigner;
  private final TypeSerializer<TimeWindow> windowSerializer;

  private transient Tuple3<Integer, Integer, Boolean> reuse;
  private transient StreamRecord<Tuple3<Integer, Integer, Boolean>> outputRecord;
  private transient InternalTimerService<TimeWindow> timerService;
  private transient InternalListState<TimeWindow, Tuple3<Integer, Integer, Long>> windowState;
  private transient ValueState<Short> itemInteractionsState;

  private transient IntCounter lateElements;

  ItemInteractionCounterTwoInputStreamOperator(short itemCut, int windowSize, TimeUnit windowUnit) {
    this.itemCut = itemCut;
    this.windowAssigner = TumblingEventTimeWindows.of(Time.of(windowSize, windowUnit));
    this.windowSerializer = windowAssigner.getWindowSerializer(null);
  }

  @Override
  public void open() throws Exception {
    this.reuse = new Tuple3<>();
    this.outputRecord = new StreamRecord<>(reuse);
    this.timerService = getInternalTimerService("window-timers", windowSerializer, this);
    this.windowState = (InternalListState<TimeWindow, Tuple3<Integer, Integer, Long>>)
        getOrCreateKeyedState(windowSerializer, new ListStateDescriptor<>(
            "windowState", new TypeHint<Tuple3<Integer, Integer, Long>>() {}.getTypeInfo()));
    this.itemInteractionsState = getKeyedStateStore().getState(new ValueStateDescriptor<>(
        "itemInteractions", Short.class));
    this.lateElements = getRuntimeContext().getIntCounter("ItemInteractionCounterLateElements");
  }

  @Override
  public void processElement1(StreamRecord<Tuple3<Integer, Integer, Long>> element) throws Exception {
    final Tuple3<Integer, Integer, Long> interaction = element.getValue();
    final long timestamp = element.getTimestamp();
    final long watermark = timerService.currentWatermark();

    if (timestamp <= watermark) {
      lateElements.add(1);
      LOG.info("Ignoring late interaction {} (timestamp: {}, watermark: {})", interaction, timestamp, watermark);
    } else {
      Collection<TimeWindow> assignedWindows = windowAssigner.assignWindows(null, timestamp, null);
      if (assignedWindows.size() > 1) {
        throw new IllegalStateException("Tumbling window but assigned interaction to multiple windows");
      }

      final TimeWindow window = assignedWindows.iterator().next();
      windowState.setCurrentNamespace(window);
      windowState.add(interaction);

      // Actual registration is only done once per (key,namespace,timestamp)
      timerService.registerEventTimeTimer(window, window.maxTimestamp());
    }
  }

  @Override
  public void processElement2(StreamRecord<Tuple2<Integer, Integer>> element) throws Exception {
    short itemInteractions = getOrDefault(itemInteractionsState, (short) 0);

    Tuple2<Integer, Integer> feedback = element.getValue();
    int item = feedback.f0;
    int increment = feedback.f1;

    if (FlinkCooccurrences.DEVELOPMENT_MODE) {
      getRuntimeContext().getIntCounter("ItemInteractionCounterFeedbackElements").add(1);

      if (itemInteractions == 0) {
        throw new IllegalStateException("Item interactions 0 for item " + item
            + ", but received decrement feedback.");
      }

      if (increment != -1) {
        throw new IllegalStateException("Received unexpected feedback " + increment);
      }
    }

    itemInteractions += increment;
    itemInteractionsState.update(itemInteractions);
  }

  @Override
  public void onEventTime(InternalTimer<Tuple, TimeWindow> timer) throws Exception {
    TimeWindow window = timer.getNamespace();
    long timestamp = window.maxTimestamp();
    windowState.setCurrentNamespace(window);

    short itemInteractions = getOrDefault(itemInteractionsState, (short) 0);

    // Update timestamp for all outgoing elements
    outputRecord.setTimestamp(timestamp);

    for (Tuple3<Integer, Integer, Long> interaction : windowState.get()) {
      if (itemInteractions < itemCut) {
        itemInteractions++;

        reuse.setFields(interaction.f0, interaction.f1, true);
        output.collect(outputRecord);
      } else {
        reuse.setFields(interaction.f0, interaction.f1, false);
        output.collect(outputRecord);
      }
    }

    itemInteractionsState.update(itemInteractions);
    windowState.clear();
  }

  @Override
  public void onProcessingTime(InternalTimer<Tuple, TimeWindow> timer) throws Exception {
    throw new IllegalStateException("How dare you use processing time?");
  }

  // -------------------------------------------------------------------------------------------------------------------

  static TypeInformation<Tuple3<Integer, Integer, Boolean>> getOutputType() {
    return new TypeHint<Tuple3<Integer, Integer, Boolean>>() {}.getTypeInfo();
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
