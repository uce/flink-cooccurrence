package com.github.uce.flinkcooccurrences;

/**
 * Utility methods for working with log-likelihood.
 *
 * <p>This class contains copied code from <code>org.apache.mahout.math.stats.LogLikelihood</code>
 * (see below for a link to the original file).
 *
 * <p>The only changes to the orginial
 *
 * @see <a href="https://git-wip-us.apache.org/repos/asf?p=mahout.git;a=blob;f=math/src/main/java/org/apache/mahout/math/stats/LogLikelihood.java;h=d2c843453b7ca8d86f3862a42eb92c9670ec7765">org.apache.mahout.math.stats.LogLikelihood</a>
 */
final class LogLikelihood {

  private LogLikelihood() {
  }

  /**
   * Calculates the Raw Log-likelihood ratio for two events, call them A and B.  Then we have:
   *
   * <table border="1" cellpadding="5" cellspacing="0"><tbody><tr><td>&nbsp;</td><td>Event
   * A</td><td>Everything but A</td></tr> <tr><td>Event B</td><td>A and B together (k_11)</td><td>B,
   * but not A (k_12)</td></tr> <tr><td>Everything but B</td><td>A without B (k_21)</td><td>Neither
   * A nor B (k_22)</td></tr></tbody> </table>
   *
   * <p>Credit to <a href="http://tdunning.blogspot.com/2008/03/surprise-and-coincidence.html">Ted
   * Dunning</a> for the table and the descriptions.
   *
   * <p>This implementation deviates from Mahout and has been transformed to require 9 instead of 11
   * calls to <code>Math.log</code>.
   *
   * @param k11 The number of times the two events occurred together
   * @param k12 The number of times the second event occurred WITHOUT the first event
   * @param k21 The number of times the first event occurred WITHOUT the second event
   * @param k22 The number of times something else occurred (i.e. was neither of these events)
   *
   * @return The raw log-likelihood ratio
   *
   * @see <a href="https://www.reddit.com/r/rust/comments/6qmnbo/why_is_my_scala_program_twice_as_fast_as_my_rust/dl0x1bj/">Why is my scala program twice as fast as my rust program? (Reddit)</a>
   */
  static double logLikelihoodRatio(long k11, long k12, long k21, long k22) {
    // note that we have counts here, not probabilities, and that the entropy is not normalized.
    final long k11k12 = k11 + k12;
    final long k21k22 = k21 + k22;

    final double all = xLogX(k11k12 + k21k22);
    final double row = all - xLogX(k11k12) - xLogX(k21k22);
    final double column = all - xLogX(k11 + k21) - xLogX(k12 + k22);
    final double matrix = all - xLogX(k11) - xLogX(k12) - xLogX(k21) - xLogX(k22);

    if (row + column < matrix) {
      // round off error
      return 0.0;
    } else {
      return 2.0 * (row + column - matrix);
    }
  }

  private static double xLogX(long x) {
    return x == 0 ? 0.0 : x * Math.log(x);
  }

}
