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

import java.util.Properties;

import com.facebook.LinkBench.ConfigUtil;

import com.facebook.LinkBench.Logger;

public class LatencyHistogramFactory {

    private final Logger logger;

    public LatencyHistogramFactory(Logger logger) {
        this.logger = logger;
    }

    /**
     * Create a latency histogram for a test given the config options
     * @param maxThreads the number of threads used by the test
     * @param props the properties from the linkbench config file
     * @return the histogram for recording latency results
     */
    public LatencyHistogram create(int maxThreads, Properties props)
    {
        String useHdr = props.getProperty("use_hdr_histogram");
        if (Boolean.valueOf(useHdr))
        {
            int histogramAccuracy = ConfigUtil.getInt(props, "hdr_histogram_accuracy");
            int maxHistogramValue = ConfigUtil.getInt(props, "hdr_histogram_max_latency");

            HdrLatencyHistogram histogram = new HdrLatencyHistogram(maxThreads, histogramAccuracy, maxHistogramValue);
            logger.info(String.format(
                "Creating HDR histogram with %d significant digits, %d max latency, and memory footprint of %d bytes",
                histogramAccuracy,
                maxHistogramValue,
                histogram.memoryUsageEstimate()));

            return histogram;
        }

        // Using default latency tracking (lower memory footprint with reduced accuracy)
        return new LatencyStats(maxThreads);
    }
}
