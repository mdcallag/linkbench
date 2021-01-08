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
package com.facebook.LinkBench;

import java.io.PrintStream;
import java.io.ByteArrayOutputStream;
import java.lang.ArrayIndexOutOfBoundsException;

import org.junit.Test;
import static org.junit.Assert.*;
import com.facebook.LinkBench.stats.HdrLatencyHistogram;
import com.facebook.LinkBench.stats.LatencyHistogram;

public class TestHdrLatencyHistogram {

  private static final String header =
      "op,count,p25 (us),p50 (us),p75 (us),p95 (us),p99 (us),max (us),mean (us),threads\n";

  @Test
  public void testSimpleDistribution() {
    LatencyHistogram histogram = new HdrLatencyHistogram(8, 3, 100);

    for(int i = 1; i <= 100; i++)
    {
      histogram.recordLatency(0, LinkBenchOp.ADD_LINK, i);
    }

    String result = getCsvResults(histogram);
    String metrics = "ADD_LINK,100,25,50,75,95,99,100,50.5,8\n";

    assertEquals(header + metrics, result);
  }

  @Test
  public void testNoDataPoints() {
    LatencyHistogram histogram = new HdrLatencyHistogram(8, 3, 100);
    String results = getCsvResults(histogram);
    assertEquals(header, results);
  }

  @Test
  public void testNoHeaderNoData() {
    LatencyHistogram histogram = new HdrLatencyHistogram(8, 3, 100);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(os);

    histogram.printCSVStats(ps, false);
    String result = os.toString();

    assertEquals("", result);
  }

  @Test
  public void testLogsLatencyValues() {
    HdrLatencyHistogram histogram = new HdrLatencyHistogram(8, 3, 5);
    histogram.recordLatency(0, LinkBenchOp.ADD_LINK, 1);
    histogram.displayLatencyStats();
  }

  @Test
  public void testLogNoStats() {
    HdrLatencyHistogram histogram = new HdrLatencyHistogram(8, 3, 5);
    histogram.displayLatencyStats();
  }

  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testOverMax() throws ArrayIndexOutOfBoundsException {
    HdrLatencyHistogram histogram = new HdrLatencyHistogram(8, 3, 5);
    histogram.recordLatency(0, LinkBenchOp.ADD_LINK, 1000000);
  }

  String getCsvResults(LatencyHistogram histogram) {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(os);
    histogram.printCSVStats(ps, true);
    return os.toString();
  }
}
