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

import java.util.Properties;

import org.junit.Test;
import static org.junit.Assert.*;

import com.facebook.LinkBench.stats.HdrLatencyHistogram;
import com.facebook.LinkBench.stats.LatencyHistogram;
import com.facebook.LinkBench.stats.LatencyHistogramFactory;
import com.facebook.LinkBench.stats.LatencyStats;

public class TestLatencyHistogramFactory {

  private static final Logger logger = Logger.getLogger();

  @Test(expected = LinkBenchConfigError.class)
  public void testMissingBothConfigs() {
    Properties props = new Properties();
    props.setProperty("use_hdr_histogram", "true");
    LatencyHistogramFactory factory = new LatencyHistogramFactory(logger);
    factory.create(8, props);
  }

  @Test(expected = LinkBenchConfigError.class)
  public void testMissingAccuracyConfig() {
    Properties props = new Properties();
    props.setProperty("use_hdr_histogram", "true");
    props.setProperty("hdr_histogram_max_latency", "8");
    LatencyHistogramFactory factory = new LatencyHistogramFactory(logger);
    factory.create(8, props);
  }

  @Test(expected = LinkBenchConfigError.class)
  public void testMissingMaxLatencyConfig() {
    Properties props = new Properties();
    props.setProperty("use_hdr_histogram", "true");
    props.setProperty("hdr_histogram_accuracy", "8");
    LatencyHistogramFactory factory = new LatencyHistogramFactory(logger);
    factory.create(8, props);
  }

  @Test(expected = LinkBenchConfigError.class)
  public void testConfigsNotNumbers() {
    Properties props = new Properties();
    props.setProperty("use_hdr_histogram", "true");
    props.setProperty("hdr_histogram_accuracy", "test");
    props.setProperty("hdr_histogram_max_latency", "test");
    LatencyHistogramFactory factory = new LatencyHistogramFactory(logger);
    factory.create(8, props);
  }

  @Test
  public void testUseHistogramNotBool() {
    Properties props = new Properties();
    props.setProperty("use_hdr_histogram", "test");
    LatencyHistogramFactory factory = new LatencyHistogramFactory(logger);

    LatencyHistogram histogram = factory.create(8, props);

    assertTrue(histogram instanceof LatencyStats);
  }

  @Test
  public void testUseHistogramFalse() {
    Properties props = new Properties();
    props.setProperty("use_hdr_histogram", "false");
    LatencyHistogramFactory factory = new LatencyHistogramFactory(logger);

    LatencyHistogram histogram = factory.create(8, props);

    assertTrue(histogram instanceof LatencyStats);
  }

  @Test
  public void testUseHistogramNothingSpecified() {
    Properties props = new Properties();
    LatencyHistogramFactory factory = new LatencyHistogramFactory(logger);
    LatencyHistogram histogram = factory.create(8, props);
    assertTrue(histogram instanceof LatencyStats);
  }


  @Test
  public void testUseHistogramAllSpecified() {
    Properties props = new Properties();
    props.setProperty("use_hdr_histogram", "true");
    props.setProperty("hdr_histogram_accuracy", "5");
    props.setProperty("hdr_histogram_max_latency", "100");
    LatencyHistogramFactory factory = new LatencyHistogramFactory(logger);

    LatencyHistogram histogram = factory.create(8, props);

    assertTrue(histogram instanceof HdrLatencyHistogram);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUseHistogramAccuracyTooHigh() {
    Properties props = new Properties();
    props.setProperty("use_hdr_histogram", "true");
    props.setProperty("hdr_histogram_accuracy", "100");
    props.setProperty("hdr_histogram_max_latency", "100");
    LatencyHistogramFactory factory = new LatencyHistogramFactory(logger);

    factory.create(8, props);
  }
}
