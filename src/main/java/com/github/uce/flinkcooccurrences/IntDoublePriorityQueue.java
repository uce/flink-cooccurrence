package com.github.uce.flinkcooccurrences;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Priority queue implementation for primitive (int, double) entries based on the
 * PriorityQueue implementation of Apache Lucene.
 *
 * <p>The implementation uses two heap arrays for storing the entries; one for the ints and one for
 * the doubles. The ordering is based on the double values.
 *
 * <p>This class contains copied code from <code>org.apache.lucene.util.PriorityQueue</code>
 * (see below for a link to the original file).
 *
 * <p>Methods that are not required in the incremental cooccurrence use case have been dropped. The
 * motivation for this primitive value variant is to reduce the pressure on the garbage collector
 * and memory consumption.
 *
 * <h3>Usage
 * <pre>
 * IntDoublePriorityQueue queue = new IntDoublePriorityQueue(k);
 *
 * if (queue.getSize() < k) {
 *   queue.add(value, score);
 * } else if (score > queue.getLeastScore()) {
 *   queue.update(value, score);
 * }
 * </pre>
 * <p>Note that the iteration order is undefined except that the first entry is the one with the
 * least score.
 *
 * <h3>Memory consumption
 *
 * <p>Each array adds 16 bytes of overhead (8 bytes for the object and 8 bytes meta data,
 * including length). The array data requires <code>maxSize</code> * 4 or <code>maxSize</code> * 8
 * bytes (4 bytes per <code>int</code> and 8 bytes per <code>double</code>).
 *
 * <p>Furthermore there is the size field of the queue, adding 4 bytes and the queue object
 * reference with 8 bytes.
 *
 * <p>This totals 8 + 4 + (16 + maxSize * 4) + (16 + maxSize * 8) bytes, e.g. 164 bytes for a
 * <code>maxSize</code> of 10 elements.
 *
 * @see <a href="https://git-wip-us.apache.org/repos/asf?p=lucene-solr.git;a=blob;f=lucene/core/src/java/org/apache/lucene/util/PriorityQueue.java;h=83ac613b676a612688e427958df82d2d82038e86">org.apache.lucene.util.PriorityQueue</a>
 */
public final class IntDoublePriorityQueue implements Iterable<IntDoublePriorityQueue.Entry> {

  /** Values of the (value, score) entries managed by this queue. */
  private final int[] values;

  /**
   * Scores of the (value, score) entries managed by this queue. Ordering happens based on this
   * array. The entries in the {@link #values} array are moved in sync with this array.
   *
   * <p>At any time (values[i], scores[i]) is a entry as added to the queue.
   */
  private final double[] scores;

  /** The current size of the priority queue. */
  private int size;

  /**
   * Creates a new {@link IntDoublePriorityQueue} instance.
   *
   * @param maxSize Maximum size of the queue (>= 1)
   */
  public IntDoublePriorityQueue(int maxSize) {
    if (maxSize < 1) {
      throw new IllegalArgumentException("maxSize not positive");
    }

    // Allocate 1 extra to avoid if statement in getLeastValue/getLeastScore() and simplified
    // implementation of downHeap() and upHeap() implementation
    scores = new double[maxSize + 1];
    values = new int[maxSize + 1];
  }

  /**
   * Returns the current size of the queue.
   *
   * @return Size of the queue
   */
  public int getSize() {
    return size;
  }

  /**
   * Returns the value with the least score in <code>O(1)</code> time.
   *
   * <p>Note: if {@link #getSize()} returns <code>0</code>, the returned value might be not be valid
   * anymore. This is due to the fact that we don't actually clear entries in {@link #reset()}, but
   * only reset the size.
   *
   * @return Value with least score
   */
  public int getLeastValue() {
    return values[1];
  }

  /**
   * Returns the least score in <code>O(1)</code> time.
   *
   * <p>Note: if {@link #getSize()} returns <code>0</code>, the returned score might be not be valid
   * anymore. This is due to the fact that we don't actually clear entries in {@link #reset()}, but
   * only reset the size.
   *
   * @return Least score
   */
  public double getLeastScore() {
    return scores[1];
  }

  /**
   * Resets the size to 0.
   *
   * <p>The array contents are not cleared.
   */
  public void reset() {
    size = 0;
  }

  /**
   * Adds an entry in <code>log(size)</code> time.
   *
   * @param value Value to add
   * @param score Score to add (ordering based on this value)
   *
   * @throws ArrayIndexOutOfBoundsException If adding more elements than the maximum size
   */
  public void add(int value, double score) {
    size++;
    values[size] = value;
    scores[size] = score;
    upHeap(size);
  }

  /**
   * Updates the queue in <code>O(log n)</code> time by replacing the least entry and
   * re-establishing the desired ordering.
   *
   * @param value Value to replace top with
   * @param score Score to replace top with
   */
  public void update(int value, double score) {
    values[1] = value;
    scores[1] = score;
    downHeap();
  }


  private void upHeap(int originalPosition) {
    int i = originalPosition;

    // Save bottom
    int value = values[i];
    double score = scores[i];

    int j = i >>> 1;
    while (j > 0 && score < scores[j]) {
      // Shift parents down
      values[i] = values[j];
      scores[i] = scores[j];
      i = j;
      j = j >>> 1;
    }

    // Install saved node
    values[i] = value;
    scores[i] = score;
  }

  private void downHeap() {
    // Save top
    int value = values[1];
    double score = scores[1];

    // Find smaller child
    int i = 1;
    int j = i << 1;
    int k = j + 1;

    if (k <= size && scores[k] < scores[j]) {
      j = k;
    }

    while (j <= size && scores[j] < score) {
      // Shift up child
      values[i] = values[j];
      scores[i] = scores[j];

      i = j;
      j = i << 1;
      k = j + 1;

      if (k <= size && scores[k] < scores[j]) {
        j = k;
      }
    }

    // Install saved node
    values[i] = value;
    scores[i] = score;
  }

  /**
   * Returns an iterator over the entries.
   *
   * <p>The first entry is the entry with the least score. The ordering of the other entries is not
   * defined.
   *
   * @return Iterator over the entries
   */
  @Override
  public Iterator<Entry> iterator() {
    return new Iterator<Entry>() {

      int i = 1;

      @Override
      public boolean hasNext() {
        return i <= size;
      }

      @Override
      public Entry next() {
        if (hasNext()) {
          Entry entry = new Entry(values[i], scores[i]);
          i++;
          return entry;
        } else {
          throw new NoSuchElementException();
        }
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  public Entry[] sortBySoreDescending() {
    Entry[] topK = new Entry[values.length];
    for (int i = 0; i < values.length; i++) {
      topK[i] = new Entry(values[i], scores[i]);
    }
    // Top score first
    Arrays.sort(topK, (o1, o2) -> Double.compare(o1.score, o2.score) * -1);
    if (topK.length == 0) {
      return topK;
    } else {
      // Skip last element (always 0)
      return Arrays.copyOfRange(topK, 0, topK.length - 1);
    }
  }

  @Override
  public String toString() {
    return Arrays.toString(sortBySoreDescending());
  }

  /**
   * An entry in the queue.
   */
  public static class Entry {

    private final int value;

    private final double score;

    public Entry(int value, double score) {
      this.value = value;
      this.score = score;
    }

    public int getValue() {
      return value;
    }

    public double getScore() {
      return score;
    }

    @Override
    public String toString() {
      return "(" + value + "," + score + ")";
    }
  }

}
