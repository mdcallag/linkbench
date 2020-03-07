/*
 * Copyright 2012, Facebook, Inc.
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An abstract class for storing both nodes and edges
import java.util.concurrent.atomic.AtomicInteger * @author tarmstrong
 */
public abstract class GraphStore extends LinkStore implements NodeStore {

  class RetryCounter {
    public RetryCounter(AtomicInteger total_ct, AtomicInteger max_ct) {
      n = 0;
      total_retries = total_ct;
      max_retries = max_ct;
    }
    private AtomicInteger total_retries;
    private AtomicInteger max_retries;
    private int n;
    public int getN() { return n; }

    public boolean inc(String caller, Logger logger) {
      n += 1;

      if (n < 2)
        return true;

      // This has a race between get and set. That is not a big problem.
      if ((n-1) > max_retries.get())
        max_retries.set(n-1);

      if (Level.TRACE.isGreaterOrEqual(debuglevel))
        logger.trace("Retry " + n + " for " + caller);

      assert(n >= 2);
      // This is initialized to 0, inc() is called on first attempt. When n >= 2 this is a retry.
      total_retries.incrementAndGet();

      if (n < 1000) {
        return true;
      } else {
        // TODO make retry configurable
        logger.error("Too many retries for " + caller);
        return false;
      }
    }
  }

  protected boolean check_count = false;
  protected Level debuglevel;
  protected Logger logger;

  public static final String CONFIG_BULK_INSERT_COUNT = "bulk_insert_count";
  public static final String CONFIG_BULK_INSERT_KB = "bulk_insert_kb";

  // Max number of Nodes, Links and Counts in a bulk load insert
  protected int bulkInsertCount = 1024;

  // Max size of Nodes in a bulk load insert. Not used to limit Link or Count
  // because they are smaller. When 0 there is no limit.
  protected int bulkInsertKB = 0;

  // Counts errors by error code
  private static HashMap<Integer, Integer> error_counts = new HashMap<Integer, Integer>();

  protected static void incError(int error_code) {
    synchronized(error_counts) {
      int count = error_counts.getOrDefault(error_code, 0) + 1;
      error_counts.put(error_code, count);
    }
  }

  protected static AtomicInteger retry_add_link = new AtomicInteger(0);
  protected static AtomicInteger retry_update_link = new AtomicInteger(0);
  protected static AtomicInteger retry_delete_link = new AtomicInteger(0);
  protected static AtomicInteger retry_get_link = new AtomicInteger(0);
  protected static AtomicInteger retry_multigetlinks = new AtomicInteger(0);
  protected static AtomicInteger retry_get_link_list = new AtomicInteger(0);
  protected static AtomicInteger retry_count_links = new AtomicInteger(0);
  protected static AtomicInteger retry_add_bulk_links = new AtomicInteger(0);
  protected static AtomicInteger retry_add_bulk_counts = new AtomicInteger(0);
  protected static AtomicInteger retry_add_node = new AtomicInteger(0);
  protected static AtomicInteger retry_bulk_add_nodes = new AtomicInteger(0);
  protected static AtomicInteger retry_get_node = new AtomicInteger(0);
  protected static AtomicInteger retry_update_node = new AtomicInteger(0);
  protected static AtomicInteger retry_delete_node = new AtomicInteger(0);

  protected static AtomicInteger max_add_link = new AtomicInteger(0);
  protected static AtomicInteger max_update_link = new AtomicInteger(0);
  protected static AtomicInteger max_delete_link = new AtomicInteger(0);
  protected static AtomicInteger max_get_link = new AtomicInteger(0);
  protected static AtomicInteger max_multigetlinks = new AtomicInteger(0);
  protected static AtomicInteger max_get_link_list = new AtomicInteger(0);
  protected static AtomicInteger max_count_links = new AtomicInteger(0);
  protected static AtomicInteger max_add_bulk_links = new AtomicInteger(0);
  protected static AtomicInteger max_add_bulk_counts = new AtomicInteger(0);
  protected static AtomicInteger max_add_node = new AtomicInteger(0);
  protected static AtomicInteger max_bulk_add_nodes = new AtomicInteger(0);
  protected static AtomicInteger max_get_node = new AtomicInteger(0);
  protected static AtomicInteger max_update_node = new AtomicInteger(0);
  protected static AtomicInteger max_delete_node = new AtomicInteger(0);

  protected static AtomicInteger retry_add_to_upd = new AtomicInteger(0);
  protected static AtomicInteger retry_upd_to_add = new AtomicInteger(0);

  protected static AtomicInteger commit_failed = new AtomicInteger(0);

  public GraphStore() {
    logger = Logger.getLogger();
  }

  public void initialize(Properties props, Phase phase, int threadId) {
    debuglevel = ConfigUtil.getDebugLevel(props);

    if (props.containsKey(Config.CHECK_COUNT))
      check_count = ConfigUtil.getBool(props, Config.CHECK_COUNT);

    if (props.containsKey(CONFIG_BULK_INSERT_COUNT))
      bulkInsertCount = ConfigUtil.getInt(props, CONFIG_BULK_INSERT_COUNT);

    if (props.containsKey(CONFIG_BULK_INSERT_KB))
      bulkInsertKB = ConfigUtil.getInt(props, CONFIG_BULK_INSERT_KB);
  }

  public int bulkLoadBatchSize() { return bulkInsertCount; }
  public int bulkLoadBatchKB() { return bulkInsertKB; }

  /** Provide generic implementation */
  public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {
    long ids[] = new long[nodes.size()];
    int i = 0;
    for (Node node: nodes) {
      long id = addNode(dbid, node);
      ids[i++] = id;
    }
    return ids;
  }

  public static void printMetrics(Logger logger) {
    logger.info("Graph Link total retry: " +
                retry_add_link.get() + " add, " +
                retry_update_link.get() + " update, " +
                retry_delete_link.get() + " delete, " +
                retry_get_link.get() + " get, " +
                retry_multigetlinks.get() + " multiget, " +
                retry_get_link_list.get() + " get_link_list, " +
                retry_count_links.get() + " count, " +
                retry_add_bulk_links.get() + " add_bulk_links, " +
                retry_add_bulk_counts.get() + " add_bulk_counts");
    logger.info("Graph Link max retry: " +
                max_add_link.get() + " add, " +
                max_update_link.get() + " update, " +
                max_delete_link.get() + " delete, " +
                max_get_link.get() + " get, " +
                max_multigetlinks.get() + " multiget, " +
                max_get_link_list.get() + " get_link_list, " +
                max_count_links.get() + " count, " +
                max_add_bulk_links.get() + " add_bulk_links, " +
                max_add_bulk_counts + " add_bulk_counts");
    logger.info("Graph Link other: " +
                retry_add_to_upd.get() + " add_to_upd, " +
                retry_upd_to_add.get() + " upd_to_add, " +
                commit_failed.get() + " commit_failed");
    logger.info("Graph Node total retry: " +
                retry_add_node.get() + " add, " +
                retry_bulk_add_nodes.get() + " add_bulk, " +
                retry_get_node.get() + " get, " +
                retry_update_node.get() + " update, " +
                retry_delete_node.get() + " delete");
    logger.info("Graph Node max retry: " +
                max_add_node.get() + " add, " +
                max_bulk_add_nodes.get() + " add_bulk, " +
                max_get_node.get() + " get, " +
                max_update_node.get() + " update, " +
                max_delete_node.get() + " delete");

    if (!error_counts.isEmpty()) {
      logger.info("Error counts");
      for (Map.Entry<Integer, Integer> entry : error_counts.entrySet())
        logger.info("error(" + entry.getKey() + ") " + entry.getValue() + " times");
    }
  }
}
