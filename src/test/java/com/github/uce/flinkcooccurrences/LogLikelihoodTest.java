package com.github.uce.flinkcooccurrences;

import org.junit.Assert;
import org.junit.Test;

public class LogLikelihoodTest {

  /**
   * Test copied and adjusted from <a href="https://github.com/sscdotopen/puppies/blob/a8c6e3a/src/llr.rs#L65">llr.rs</a>.
   */
  @Test
  public void testLogLikelihoodRatio() {
    // Cases from Ted's paper http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.14.5962
    Assert.assertEquals(270.72, LogLikelihood.logLikelihoodRatio(110, 2442, 111, 29114), 0.1d);
    Assert.assertEquals(263.90, LogLikelihood.logLikelihoodRatio(29, 13, 123, 31612), 0.1d);
    Assert.assertEquals(48.94, LogLikelihood.logLikelihoodRatio(9, 12, 429, 31327), 0.1d);
  }
}
