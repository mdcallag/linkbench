package com.facebook.LinkBench;

import java.io.IOException;
import java.util.Properties;

public class MemoryNodeStoreTest extends NodeStoreTestBase {
  MemoryLinkStore store;
  @Override
  protected void initNodeStore(Properties props) throws Exception, IOException {
    store = new MemoryLinkStore();
    store.initialize(props, Phase.REQUEST, 0);
  }

  @Override
  protected NodeStore getNodeStoreHandle(boolean initialized) throws Exception, IOException {
    return new DummyLinkStore(store.newHandle(), initialized);
  }

}