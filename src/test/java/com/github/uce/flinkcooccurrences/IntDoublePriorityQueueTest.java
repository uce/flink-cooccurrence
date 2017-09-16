package com.github.uce.flinkcooccurrences;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

public class IntDoublePriorityQueueTest {

  @Test
  public void testAddAscendingOrder() {
    int maxSize = 10;
    IntDoublePriorityQueue queue = new IntDoublePriorityQueue(maxSize);
    for (int i = 0; i < maxSize; i++) {
      queue.add(i, i);
    }

    Assert.assertEquals(0, queue.getLeastValue());
    Assert.assertEquals(0.0, queue.getLeastScore(), 0.0);
  }

  @Test
  public void testAddDescendingOrder() {
    int maxSize = 10;
    IntDoublePriorityQueue queue = new IntDoublePriorityQueue(maxSize);
    for (int i = maxSize - 1; i >= 0; i--) {
      queue.add(i, i);
    }

    Assert.assertEquals(0, queue.getLeastValue());
    Assert.assertEquals(0.0, queue.getLeastScore(), 0.0);
  }

  @Test
  public void testRandomElements() {
    Random random = new Random(0xC0FFEE);
    int maxSize = 10;
    int n = 100;

    double[] scores = new double[n];
    for (int i = 0; i < n; i++) {
      scores[i] = random.nextDouble();
    }

    IntDoublePriorityQueue queue = new IntDoublePriorityQueue(maxSize);
    for (int i = 0; i < n; i++) {
      if (queue.getSize() < maxSize) {
        queue.add(i, scores[i]);
      } else if (scores[i] > queue.getLeastScore()) {
        queue.update(i, scores[i]);
      }
    }

    // Verify least score
    Arrays.sort(scores);
    Assert.assertEquals(scores[n - maxSize], queue.getLeastScore(), 0.0);

    // Verify top K
    double[] topK = new double[maxSize];
    Iterator<IntDoublePriorityQueue.Entry> iterator = queue.iterator();

    for (int i = 0; i < maxSize; i++) {
      topK[i] = iterator.next().getScore();
    }

    Assert.assertEquals(scores[n - maxSize], topK[0], 0.0);

    // Sort and verify complete top K
    Arrays.sort(topK);
    for (int i = 0; i < maxSize; i++) {
      Assert.assertEquals(scores[n - maxSize + i], topK[i], 0.0);
    }
  }

  @Test
  public void testAddAndClear() {
    int maxSize = 10;
    IntDoublePriorityQueue queue = new IntDoublePriorityQueue(maxSize);

    queue.add(0, 0.0);
    queue.add(1, 1.0);
    queue.add(2, 2.0);

    Assert.assertEquals(3, queue.getSize());

    queue.reset();

    for (int i = 0; i < maxSize; i++) {
      queue.add(i, i);
    }

    Assert.assertEquals(maxSize, queue.getSize());

    Assert.assertEquals(0, queue.getLeastValue());
    Assert.assertEquals(0.0, queue.getLeastScore(), 0.0);
  }
}
