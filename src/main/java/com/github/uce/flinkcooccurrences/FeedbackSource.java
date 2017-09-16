package com.github.uce.flinkcooccurrences;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.BlockingQueueBroker;

/**
 * A source that can receive feedback from an operator in the same JVM.
 *
 * @param <T> Type of collected feedback
 */
class FeedbackSource<T> extends RichParallelSourceFunction<T> {

  private static final long serialVersionUID = 3093447861954390123L;

  private final UUID feedbackId;

  private transient BlockingQueue<?> feedbackQueue;

  private volatile boolean isRunning = true;

  FeedbackSource(UUID feedbackId) {
    this.feedbackId = feedbackId;
  }

  static String getBrokerString(UUID feedbackId, int subtaskIndex) {
    return feedbackId.toString() + "-" + subtaskIndex;
  }

  @Override
  public void open(Configuration parameters) {
    this.feedbackQueue = new LinkedTransferQueue<>();
    String brokerString = getBrokerString(feedbackId, getRuntimeContext().getIndexOfThisSubtask());
    BlockingQueueBroker.INSTANCE.handIn(brokerString, feedbackQueue);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void run(SourceContext<T> ctx) throws Exception {
    ctx.emitWatermark(new Watermark(Long.MAX_VALUE));

    while (isRunning) {
      Object element = feedbackQueue.take();

      if (element instanceof FeedbackSource.EndOfFeedback) {
        return;
      } else {
        synchronized (ctx.getCheckpointLock()) {
          ctx.collect((T) element);
        }
      }
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }

  enum EndOfFeedback {

    Instance

  }
}
