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

import com.facebook.LinkBench.LinkBenchOp;

public interface LatencyHistogram {

    /**
     * Record a latency value from a test
     * @param threadid the thread that is reporting the latency
     * @param type the type of operation that the latency is from
     * @param microtimetaken the time taken, in microseconds, for the operation to complete
     */
    public void recordLatency(int threadid, LinkBenchOp type,
        long microtimetaken);

    /**
     * Print statistics about latency to the logs
     */
    public void displayLatencyStats();

    /**
     * Save the latency results in csv format
     * @param out the stream to write the csv data to
     * @param header true if the header values of the csv should be printed
     */
    public void printCSVStats(PrintStream out, boolean header);
}
