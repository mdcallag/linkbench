package com.facebook.LinkBench;

import com.mongodb.client.MongoDatabase;

import java.io.IOException;
import java.util.Properties;


public class MongoDbNodeStoreTestBasic extends NodeStoreTestBase {

    MongoDatabase conn;
    Properties currProps;

    @Override
    protected Properties basicProps() {
        Properties props = super.basicProps();
        MongoDbTestConfigBasic.fillMongoDbTestServerProps(props);
        return props;
    }

    @Override
    protected void initNodeStore(Properties props) throws Exception, IOException {
        currProps = props;
        conn = MongoDbTestConfigBasic.createConnection(testDB);
        MongoDbTestConfigBasic.dropTestTables(conn, testDB);
        MongoDbTestConfigBasic.createTestTables(conn, testDB);
    }

    @Override
    protected NodeStore getNodeStoreHandle(boolean initialize) throws Exception, IOException {
        DummyLinkStore result = new DummyLinkStore(new LinkStoreMongoDbBasic());
        if (initialize) {
            result.initialize(currProps, Phase.REQUEST, 0);
        }
        return result;
    }

}

