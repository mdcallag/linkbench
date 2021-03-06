package com.facebook.LinkBench;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Map;
import java.util.Properties;

import static com.facebook.LinkBench.Config.*;
import static com.facebook.LinkBench.GraphStore.*;
import static com.facebook.LinkBench.LinkStoreMongoDb2.*;
import static com.mongodb.client.model.Updates.combine;

/**
 * Class containing hardcoded parameters and helper functions used to create
 * and connect to the unit test database for MongoDB.
 */
public class MongoDbTestConfig2 {

    // Hardcoded parameters for now
    @SuppressWarnings("FieldCanBeLocal")
    private static String host = "localhost";

    @SuppressWarnings("FieldCanBeLocal")
    private static int port = 27017;

    @SuppressWarnings("FieldCanBeLocal")
    private static String user = "linkbench";

    @SuppressWarnings("FieldCanBeLocal")
    private static String pass = "linkbench";

    private static String url = "mongodb://localhost:27017/replset";
    private static String linktable = "test_linktable";
    private static String counttable = "test_counttable";
    private static String nodetable = "test_nodetable";

    static boolean getBooleanOrDefault(String name, boolean missing) {
        try {
            Map<String, String> env = System.getenv();
            if(env.containsKey(name)) {
                return Boolean.valueOf(env.get(name));
            } else {
                return missing;
            }
        } catch (Exception e ) {
        }
        return missing;
    }
    static int getIntegerOrDefault(String name, int missing) {
        try {
            Map<String, String> env = System.getenv();
            if(env.containsKey(name)) {
                return Integer.valueOf(env.get(name));
            } else {
                return missing;
            }
        } catch (Exception e ) {
        }
        return missing;
    }
    static void fillMongoDbTestServerProps(Properties props) {
        props.setProperty(LINKSTORE_CLASS, LinkStoreMongoDb2.class.getName());
        props.setProperty(NODESTORE_CLASS, LinkStoreMongoDb2.class.getName());
        props.setProperty(CONFIG_HOST, host);
        props.setProperty(CONFIG_PORT, Integer.toString(port));
        props.setProperty(CONFIG_USER, user);
        props.setProperty(CONFIG_PASSWORD, pass);
        props.setProperty(CONFIG_URL, url);
        props.setProperty(CHECK_COUNT, Boolean.toString(getBooleanOrDefault(CHECK_COUNT, true)));

        props.setProperty(REQUEST_RANDOM_SEED, "12020569");
        props.setProperty(LOAD_RANDOM_SEED, "26854520010");

        props.setProperty(Config.LINK_TABLE, linktable);
        props.setProperty(Config.COUNT_TABLE, counttable);
        props.setProperty(Config.NODE_TABLE, nodetable);
    }

    static MongoDatabase createConnection(String testDB) {
        return new MongoClient(new MongoClientURI(url)).getDatabase(testDB);
    }


    private static MongoCollection<Document> createTestCollection(MongoDatabase database, String name) {
        database.createCollection(name);
        return database.getCollection(name);
    }

    private static void createIndex(MongoCollection<Document> collection, Bson index, boolean unqiue) {
        collection.createIndex(index, new IndexOptions().unique(unqiue));
    }

    static void createTestTables(MongoDatabase database, String testDB) {
        MongoCollection<Document> collection;

        collection = createTestCollection(database,MongoDbTestConfig2.linktable);
        createIndex(collection,
                    combine(Indexes.ascending("id1"),
                            Indexes.ascending("link_type"),
                            Indexes.ascending("visibility"),
                            Indexes.ascending("time"),
                            Indexes.ascending("id2"),
                            Indexes.ascending("version"),
                            Indexes.ascending("data")),
             false);

        collection = createTestCollection(database, MongoDbTestConfig2.nodetable);

        collection = createTestCollection(database, MongoDbTestConfig2.counttable);

    }

    static void dropTestTables(MongoDatabase conn, String testDB) {
        conn.getCollection(MongoDbTestConfig2.linktable).drop();
        conn.getCollection(MongoDbTestConfig2.nodetable).drop();
        conn.getCollection(MongoDbTestConfig2.counttable).drop();
    }
}
