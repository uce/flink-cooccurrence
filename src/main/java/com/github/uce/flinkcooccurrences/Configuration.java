package com.github.uce.flinkcooccurrences;

import java.io.PrintWriter;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;

/**
 * Configuration of a {@link FlinkCooccurrences} run.
 */
final class Configuration {

  private final URI input;

  private final boolean skipCuts;
  private final short fMax;
  private final short kMax;
  private final short topK;

  private final int windowSize;
  private final TimeUnit windowUnit;

  private final long seed;

  private final long bufferTimeout;

  private Configuration(
      URI input,
      boolean skipCuts,
      short fMax,
      short kMax,
      short topK,
      int windowSize,
      TimeUnit windowUnit,
      long seed,
      long bufferTimeout) {
    this.input = input;
    this.skipCuts = skipCuts;
    this.fMax = fMax;
    this.kMax = kMax;
    this.topK = topK;
    this.windowSize = windowSize;
    this.windowUnit = windowUnit;
    this.seed = seed;
    this.bufferTimeout = bufferTimeout;
  }

  static Configuration fromArgs(String[] args) {
    Option inputOption = Option.builder("i")
        .longOpt("input")
        .desc("Input file/directory to consume (expected format 'user,item,timestamp')")
        .hasArg()
        .required()
        .build();

    Option skipCutsOption = Option.builder("sc")
        .longOpt("skip-cuts")
        .desc("Skip the interaction cuts")
        .build();

    Option fMaxOption = Option.builder("ic")
        .longOpt("item-cut")
        .desc("Item interaction cut")
        .hasArg()
        .build();

    Option kMaxOption = Option.builder("uc")
        .longOpt("user-cut")
        .desc("User interaction cut")
        .hasArg()
        .build();

    Option topKOption = Option.builder("k")
        .longOpt("top-k")
        .desc("Top K")
        .hasArg()
        .build();

    Option windowSizeOption = Option.builder("ws")
        .longOpt("window-size")
        .desc("Window size")
        .required()
        .hasArg()
        .build();

    Option windowUnitOption = Option.builder("wu")
        .longOpt("window-unit")
        .desc("TimeUnit for the window (default: milliseconds)")
        .hasArg()
        .build();

    Option seedOption = Option.builder("s")
        .longOpt("seed")
        .desc("Seed for random number generator")
        .hasArg()
        .build();

    Option bufferTimeoutOption = Option.builder("bt")
        .longOpt("buffer-timeout")
        .desc("Buffer timeout (default: 100ms)")
        .hasArg()
        .build();

    Option helpOption = Option.builder("h")
        .longOpt("help")
        .desc("Print help")
        .build();

    Options options = new Options();
    options.addOption(inputOption);
    options.addOption(skipCutsOption);
    options.addOption(fMaxOption);
    options.addOption(kMaxOption);
    options.addOption(topKOption);
    options.addOption(windowSizeOption);
    options.addOption(windowUnitOption);
    options.addOption(seedOption);
    options.addOption(bufferTimeoutOption);
    options.addOption(helpOption);

    Options helpOptions = new Options();
    helpOptions.addOption(helpOption);

    CommandLineParser parser = new DefaultParser();
    try {
      // Help first :(
      CommandLine cmd = parser.parse(helpOptions, args, true);

      if (cmd.hasOption(helpOption.getOpt())) {
        printHelpToStdErr(options, null);
        System.exit(0);
      }
    } catch (Exception e) {
      printHelpToStdErr(options, e);
      System.exit(1);
    }

    try {
      // Main thing...
      CommandLine cmd = parser.parse(options, args);
      URI input = URI.create(cmd.getOptionValue(inputOption.getOpt()));
      boolean skipCuts = cmd.hasOption(skipCutsOption.getOpt());
      short fMax = getShort(cmd, fMaxOption, (short) 500);
      short kMax = getShort(cmd, kMaxOption, (short) 500);
      short topK = getShort(cmd, topKOption, (short) 10);

      int windowSize = Integer.valueOf(cmd.getOptionValue(windowSizeOption.getOpt()));

      TimeUnit windowUnit = TimeUnit.MILLISECONDS;
      String windowUnitString = cmd.getOptionValue(windowUnitOption.getOpt());
      if (windowUnitString != null) {
        switch (windowUnitString.toUpperCase()) {
          case "MILLISECONDS":
            windowUnit = TimeUnit.MILLISECONDS;
            break;
          case "SECONDS":
            windowUnit = TimeUnit.SECONDS;
            break;
          case "MINUTES":
            windowUnit = TimeUnit.MINUTES;
            break;
          case "HOURS":
            windowUnit = TimeUnit.HOURS;
            break;
          case "DAYS":
            windowUnit = TimeUnit.DAYS;
            break;
          default:
            throw new IllegalArgumentException("Unrecognized window unit " + windowUnitString);
        }
      }

      long seed = getLongHex(cmd, seedOption, System.nanoTime());
      long bufferTimeout = getLong(cmd, bufferTimeoutOption, 100L);

      return new Configuration(
          input,
          skipCuts, fMax,
          kMax,
          topK,
          windowSize,
          windowUnit,
          seed,
          bufferTimeout);
    } catch (Exception e) {
      printHelpToStdErr(options, e);
    }

    System.exit(1);
    throw new IllegalStateException();
  }

  private static short getShort(CommandLine cmd, Option option, short defaultValue) {
    return Optional.ofNullable(cmd.getOptionValue(option.getOpt()))
        .map(Short::valueOf).orElse(defaultValue);
  }

  private static long getLong(CommandLine cmd, Option option, long defaultValue) {
    return Optional.ofNullable(cmd.getOptionValue(option.getOpt()))
        .map(Long::valueOf).orElse(defaultValue);
  }

  private static long getLongHex(CommandLine cmd, Option option, long defaultValue) {
    return Optional.ofNullable(cmd.getOptionValue(option.getOpt()))
        .map((value) -> {
          if (value.startsWith("0x")) {
            return Long.parseLong(value.substring(2), 16);
          } else {
            return Long.valueOf(value);
          }
        }).orElse(defaultValue);
  }

  private static void printHelpToStdErr(Options options, @Nullable Exception cause) {
    HelpFormatter help = new HelpFormatter();
    help.printHelp(
        new PrintWriter(System.err, true),
        200,
        "FlinkCooccurrences",
        cause != null ? cause.getMessage() : null,
        options,
        5,
        5,
        "",
        true);
  }

  URI getInput() {
    return input;
  }

  public boolean getSkipCuts() {
    return skipCuts;
  }

  short getItemCut() {
    return fMax;
  }

  short getUserCut() {
    return kMax;
  }

  short getTopK() {
    return topK;
  }

  int getWindowSize() {
    return windowSize;
  }

  public TimeUnit getWindowUnit() {
    return windowUnit;
  }

  long getSeed() {
    return seed;
  }

  public long getBufferTimeout() {
    return bufferTimeout;
  }

  void logConfiguration(Logger logger) {
    logger.info("input\t{}", input);
    logger.info("skip cuts\t{}", skipCuts);
    logger.info("item cut (fMax)\t{}", fMax);
    logger.info("user cut (kMax)\t{}", kMax);
    logger.info("topK\t{}", topK);
    logger.info("windowSize\t{}", windowSize);
    logger.info("windowUnit\t{}", windowUnit);
    logger.info("seed\t{}", seed);
    logger.info("buffer timeout\t{}", bufferTimeout);
  }

  @Override
  public String toString() {
    return "Configuration{" +
        "input=" + input +
        ", skipCuts=" + skipCuts +
        ", fMax=" + fMax +
        ", kMax=" + kMax +
        ", topK=" + topK +
        ", windowSize=" + windowSize +
        ", windowUnit=" + windowUnit +
        ", seed=0x" + Long.toHexString(seed) +
        ", bufferTimeout=" + bufferTimeout +
        '}';
  }

  public static void main(String[] args) {
    Configuration c = Configuration.fromArgs(args);
    System.out.println(c);
  }
}
