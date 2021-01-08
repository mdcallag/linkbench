/*
 * Copyright 2021-present MongoDB Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.LinkBench.stats;

import java.io.PrintStream;
import java.text.DecimalFormat;

import com.facebook.LinkBench.LinkBenchOp;
import com.facebook.LinkBench.LinkStore;
import com.facebook.LinkBench.Logger;
import org.HdrHistogram.Histogram;

/**
 * Class used to track latency values using HDR Histogram.  This provides
 * a more accurate estimate of the latency percentiles at a cost of a higher
 * memory footprint than the default histogram type
 *
 * See http://hdrhistogram.org/ for more details
 */
public class HdrLatencyHistogram implements LatencyHistogram {

  private static final int percentiles[] = new int[] {25, 50, 75, 95, 99};
  private final Histogram histograms[];
  private final int maxThreads;

  public HdrLatencyHistogram(int maxThreads, int histogramAccuracy, int maxHistogramValue) {
    this.maxThreads = maxThreads;
    histograms = new Histogram[LinkStore.MAX_OPTYPES];
    for (LinkBenchOp op : LinkBenchOp.values()) {
      histograms[op.ordinal()] = new Histogram(maxHistogramValue, histogramAccuracy);
    }
  }

  /**
   * Get an estimate of the amount of memory that will be used by the histogram
   */
  public long memoryUsageEstimate() {
    long usageInBytes = 0;
    for(Histogram histogram : histograms)
    {
      usageInBytes += histogram.getEstimatedFootprintInBytes();
    }

    return usageInBytes;
  }

  /**
   * Track a latency value for a specific operation
   */
  @Override
  public void recordLatency(int threadid, LinkBenchOp type, long microtimetaken) {
    histograms[type.ordinal()].recordValue(microtimetaken);
  }

  /**
   * Print the current latency stats tracked to the logs
   */
  @Override
  public void displayLatencyStats() {
    Logger logger = Logger.getLogger();
    // print percentiles
    for (LinkBenchOp type: LinkBenchOp.values()) {

      Histogram histogram = histograms[type.ordinal()];
      if (histogram.getTotalCount() == 0) { // no samples of this type
        continue;
      }

      long sampleCounts = histogram.getTotalCount();
      String logString = type.displayName() + String.format(" count = %d ", sampleCounts);
      for(int percentile: percentiles) {
        logString += String.format(" p%d = %dus ", percentile, histogram.getValueAtPercentile(percentile));
      }

      double mean = histogram.getMean();
      long max = histogram.getMaxValue();
      logString += String.format(" max = %dus mean = %.3fus threads = %d", max, mean, maxThreads);
      logger.info(logString);
    }
  }

  /**
   * Print the current latency stats in csv format
   */
  @Override
  public void printCSVStats(PrintStream out, boolean header) {
    printCSVStats(out, header, LinkBenchOp.values());
  }

  private void printCSVStats(PrintStream out, boolean header, LinkBenchOp... ops) {

    // Write out the header
    if (header) {
      out.print("op,count");
      for (int percentile: percentiles) {
        out.print(String.format(",p%d (us)", percentile));
      }

      out.print(",max (us),mean (us),threads");
      out.println();
    }

    DecimalFormat df = new DecimalFormat("#.##");
    for (LinkBenchOp op: ops) {
      Histogram histogram = histograms[op.ordinal()];
      long samples = histogram.getTotalCount();
      if (samples == 0) {
        continue;
      }

      out.print(op.name());
      out.print(",");
      out.print(samples);

      for (int percentile: percentiles) {
        double percentileValue = histogram.getValueAtPercentile(percentile);
        out.print(",");
        out.print(df.format(percentileValue));
      }

      String max = df.format(histogram.getMaxValueAsDouble());
      String mean = df.format(histogram.getMean());

      out.print(",");
      out.print(max);
      out.print(",");
      out.print(mean);
      out.print(",");
      out.print(maxThreads);
      out.println();
    }
  }
}
