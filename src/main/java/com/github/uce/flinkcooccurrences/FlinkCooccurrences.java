package com.github.uce.flinkcooccurrences;

import it.unimi.dsi.fastutil.ints.Int2ShortOpenHashMap;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperator;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkCooccurrences {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkCooccurrences.class);

  static final boolean DEVELOPMENT_MODE = false;

  public static void main(String[] args) throws Exception {
    Configuration configuration = Configuration.fromArgs(args);
    configuration.logConfiguration(LOG);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.registerTypeWithKryoSerializer(ItemCooccurrences.class, ItemCooccurrences.Serializer.class);
    env.getConfig().enableObjectReuse();
    env.setMaxParallelism(env.getParallelism());
    env.setBufferTimeout(configuration.getBufferTimeout());

    boolean skipCuts = configuration.getSkipCuts();
    int windowSize = configuration.getWindowSize();
    TimeUnit windowUnit = configuration.getWindowUnit();

    // ------------------------
    // Main source
    // ------------------------
    DataStream<Tuple3<Integer, Integer, Long>> interactionStream =
        createFileInput(env, configuration.getInput())
            .map(new InteractionLineSplitter())
            .name("LineSplitter")
            .assignTimestampsAndWatermarks(new InteractionEventTimeAssigner())
            .startNewChain()
            .name("InteractionEventTimeAssigner");

    SingleOutputStreamOperator<Void> sampledInteractions;

    if (skipCuts) {
      sampledInteractions = interactionStream
          // ------------------------
          // User interactions (non-sampled)
          // ------------------------
          .keyBy(0)
          .transform(
              "NonSampledUserInteractionCounter (" + windowSize + " " + windowUnit + ")",
              NonSampledUserInteractionCounterOneInputStreamOperator.getOutputType(),
              new NonSampledUserInteractionCounterOneInputStreamOperator(windowSize, windowUnit));
    } else {
      // ------------------------
      // Feedback source
      // ------------------------
      UUID feedbackId = UUID.randomUUID();

      DataStream<Tuple2<Integer, Integer>> feedbackStream = env
          .addSource(new FeedbackSource<Tuple2<Integer, Integer>>(feedbackId))
          .returns(new TypeHint<Tuple2<Integer, Integer>>() {}.getTypeInfo())
          .name("FeedbackSource (" + feedbackId + ")");

      // ------------------------
      // Interaction sampling
      // ------------------------
      KeyedStream<Tuple2<Integer, Integer>, Tuple> feedbackByItem = feedbackStream.keyBy(0);
      KeyedStream<Tuple3<Integer, Integer, Long>, Tuple> interactionsByItem = interactionStream.keyBy(1);

      sampledInteractions =
          interactionsByItem.connect(feedbackByItem)
              // ------------------------
              // Item sampling
              // ------------------------
              .transform(
                  "ItemInteractionCounter (" + windowSize + " " + windowUnit + ")",
                  ItemInteractionCounterTwoInputStreamOperator.getOutputType(),
                  new ItemInteractionCounterTwoInputStreamOperator(
                      configuration.getItemCut(),
                      windowSize,
                      windowUnit
                  ))
              // ------------------------
              // User sampling
              // ------------------------
              .keyBy(0)
              .transform(
                  "UserInteractionCounter (" + windowSize + " " + windowUnit + ")",
                  UserInteractionCounterOneInputStreamOperator.getOutputType(),
                  new UserInteractionCounterOneInputStreamOperator(
                      feedbackId,
                      configuration.getUserCut(),
                      configuration.getSeed(),
                      windowSize,
                      windowUnit));

      // Sanity check for co-location of FeedbackSource and the UserInteractionCounter
      if (feedbackStream.getParallelism() != sampledInteractions.getParallelism()) {
        throw new IllegalStateException("Parallelism mismatch for FeedbackSource and UserInteractionCounter");
      }

      // This is custom work around of the Flink version we use here
      // We need the feedback source and the user interaction counter
      // to be co-located.
      //
      // TODO: Build on top colocating Flink
      // env.colocateTransformations("FeedbackSource", "UserInteractionCounter");
    }

    // ------------------------
    // Row sums
    // ------------------------
    DataStream<Tuple2<Integer, Integer>> rowSumUpdateStream = sampledInteractions.getSideOutput(
        UserInteractionCounterOneInputStreamOperator.getRowSumTag());

    DataStream<Tuple2<Integer, Integer>> rowSumStream = rowSumUpdateStream.keyBy(0).window(
        TumblingEventTimeWindows.of(Time.of(windowSize, windowUnit)))
        .aggregate(
            new RowSumAggregator.RowSumAggregateFunction(),
            new RowSumAggregator.RowSumProcessWindow())
        .name("RowSumAggregator");

    // ------------------------
    // Co-occurrence matrix
    // ------------------------
    DataStream<ItemCooccurrences> itemCooccurrencesStream = sampledInteractions.getSideOutput(
        UserInteractionCounterOneInputStreamOperator.getItemCooccurrencesTag());

    DataStream<Tuple2<Integer, Int2ShortOpenHashMap>> itemCooccurrenceRowStream = itemCooccurrencesStream
        .keyBy(ItemCooccurrences::getItem)
        .window(TumblingEventTimeWindows.of(Time.of(windowSize, windowUnit)))
        .aggregate(
            new ItemRowAggregator.ItemCooccurrenceRowAggregateFunction(),
            new ItemRowAggregator.ItemCooccurrenceRowWindowFunction())
        .name("ItemRowAggregator");

    // ------------------------
    // Rescoring
    // ------------------------
    DataStream<Tuple2<Integer, IntDoublePriorityQueue>> topKStream = itemCooccurrenceRowStream
        .keyBy(0).connect(rowSumStream.broadcast())
        .transform(
            "ItemRowRescorer",
            ItemRowRescorerTwoInputStreamOperator.getOutputType(),
            new ItemRowRescorerTwoInputStreamOperator(configuration.getTopK()));

    // Need to add this in order to work around https://issues.apache.org/jira/browse/FLINK-3974
    // Alternative: disable object reuse
    topKStream.addSink((ignored) -> {});

    long start = System.nanoTime();
    JobExecutionResult executionResult = env.execute(
        "FlinkCooccurrences");
    long end = System.nanoTime();

    long duration = TimeUnit.NANOSECONDS.toMillis(end - start);
    LOG.info("Duration\t{}", duration);

    LOG.info("Accumulator results: {}", executionResult.getAllAccumulatorResults().toString());
  }

  // ------------------------
  // Main source helpers
  // ------------------------

  private static DataStreamSource<String> createFileInput(StreamExecutionEnvironment env, URI input) {
    UnsplittableTextInputFormat inputFormat = new UnsplittableTextInputFormat(input);

    ContinuousFileMonitoringFunction<String> monitoringFunction =
        new ContinuousFileMonitoringFunction<>(
            inputFormat,
            FileProcessingMode.PROCESS_ONCE,
            env.getParallelism(),
            0);

    ContinuousFileReaderOperator<String> reader = new ContinuousFileReaderOperator<>(inputFormat);

    SingleOutputStreamOperator<String> source = env
        .addSource(monitoringFunction).name("SplitReader")
        .transform("InputReader(" + input + ")", BasicTypeInfo.STRING_TYPE_INFO, reader);

    return new DataStreamSource<>(source);
  }

  private static class InteractionLineSplitter implements MapFunction<String, Tuple3<Integer, Integer, Long>> {

    private static final long serialVersionUID = 8545641330133172233L;

    private final Tuple3<Integer, Integer, Long> reuse = new Tuple3<>();

    @Override
    public Tuple3<Integer, Integer, Long> map(String line) throws Exception {
      String[] split = line.split(",");
      reuse.setFields(Integer.valueOf(split[0]), Integer.valueOf(split[1]), Long.valueOf(split[2]));
      return reuse;
    }
  }

  private static class InteractionEventTimeAssigner extends AscendingTimestampExtractor<Tuple3<Integer, Integer, Long>> {

    private static final long serialVersionUID = 4519836778231287363L;

    @Override
    public long extractAscendingTimestamp(Tuple3<Integer, Integer, Long> element) {
      return element.f2;
    }
  }

}
