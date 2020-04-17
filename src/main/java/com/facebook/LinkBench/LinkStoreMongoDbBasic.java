/**
 * This is a basic port of LinkStoreMysql to MongoDB. See LinkStoreMongoDb2 for a more optimized version.
 *
 * You need to create the following indexes to run this version:
 *
 *     use linkdb0
 *     db.linktable.createIndex({id1: 1, link_type: 1, id2: 1}, {unique: true});
 *     db.linktable.createIndex({id1: 1, link_type: 1, time: 1, visibility: 1});
 *     db.counttable.createIndex({id: 1, link_type: 1}, {unique: true});
 *     db.nodetable.createIndex({id: 1}, {unique: true});
 */
package com.facebook.LinkBench;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import org.bson.BsonBinary;
import org.bson.BsonDocument;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.ConnectionString;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.vault.ClientEncryptions;
import com.mongodb.client.vault.ClientEncryption;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.exclude;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Updates.*;
import static java.util.Arrays.asList;

import java.nio.ByteBuffer;

import org.bson.*;
import org.bson.Document;

import org.bson.conversions.Bson;
import org.bson.types.Binary;

class UuidUtils {
    public static UUID asUuid(byte[] bytes) {
      ByteBuffer bb = ByteBuffer.wrap(bytes);
      long firstLong = bb.getLong();
      long secondLong = bb.getLong();
      return new UUID(firstLong, secondLong);
    }

    public static byte[] asBytes(UUID uuid) {
      ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());
      return bb.array();
    }
  }

/**
 * LinkStore implementation for MongoDB, using {@link ClientSession#startTransaction()},
 * {@link ClientSession#commitTransaction()} and {@link ClientSession#abortTransaction()}.
 *
 * @see com.mongodb.client.ClientSession
 */
public class LinkStoreMongoDbBasic extends GraphStore {

    /* MongoDB database server configuration keys */
    static final String CONFIG_URL = "url";
    static final String CONFIG_HOST = "host";
    static final String CONFIG_PORT = "port";
    static final String CONFIG_USER = "user";
    static final String CONFIG_PASSWORD = "password";
    static final String MAX_RETRIES = "max_retries";
    static final String BULKINSERT_SIZE = "bulkinsert_size";
    static final String FLE_ENABLE = "fle_enable";

    // only valid for profiling the tests
    static final String SKIP_TRANSACTIONS = "skip_transactions";

    static final int DEFAULT_BULKINSERT_SIZE = 1024;
    static final boolean DEFAULT_SKIP_TRANSACTIONS = false;
    static final int DEFAULT_MAX_RETRIES = 16;

    private boolean skip_transactions = DEFAULT_SKIP_TRANSACTIONS;

    private static final long NODE_GEN_UNINITIALIZED = -1L;

    // In MongoDB, these will be "collections", which are the analog of SQL "tables". We call them
    // "tables" here to keep the naming convention consistent across different database benchmarks.
    private String linktable;
    private String counttable;
    private String nodetable;

    private String url;
    private String host;
    private int port;
    private String user;
    private char[] pwd;
    private int max_retries;
    private boolean use_encryption = false;

    private ClientSession session;
    private MongoClient mongoClient;

    // A monotonically increasing counter used to assign unique graph node ids. We make it atomic in case it is
    // accessed from multiple threads. We use this since there is no default way to have an 'auto-incrementing' field
    // in MongoDB documents.
    private static AtomicLong nodeIdGen =  new AtomicLong(NODE_GEN_UNINITIALIZED);

    private int bulkInsertSize = DEFAULT_BULKINSERT_SIZE;

    private final Logger logger = Logger.getLogger();
    private boolean debug = false; // true if debug log level is enabled

    LinkStoreMongoDbBasic() {
        super();
    }

    @SuppressWarnings("unused")
    LinkStoreMongoDbBasic(Properties props) {
        super();
        initialize(props, Phase.LOAD, 0);
    }

    // Workaround CDRIVER-3338
    private byte[] getNonEmptyData(byte[] data) {
        if (use_encryption == true && data.length == 0) {
            return new byte[] { 0x65};
        }

        return data;
    }

    private static String generateNodeSchema(String keyId) {
        // Generate a JSON schema for the following collection.
        // Some fields are not marked as encrypted because they use query/update operators which
        // are not supported by FLE.
        // class Node {
        //     public long id;       // Unique identifier for node
        //     public int type;      // Type of node
        //     public long version;  // Version, incremented each change
        //     public int time;      // Last modification time
        //     public byte data[];   // Arbitrary payload data
        //   }
        String[][] fields = {
            //{ "id", "long"}, - we sort on this key
            { "type", "int"},
            { "version", "long"},
            { "time", "int"},
            { "data", "binData"},
        };

        return generateSchema(keyId, fields);
    }

    private static String generateLinkSchema(String keyId) {
        // Generate a JSON schema for the following collection.
        // Some fields are not marked as encrypted because they use query/update operators which
        // are not supported by FLE.
        // class Link {
        //     public long id1;        // id of source node
        //     public long link_type;  // type of link
        //     public long id2;        // id of destination node
        //     public byte visibility; // is link visible?
        //     public byte[] data;     // arbitrary data (must be short)
        //     public int version;     // version of link
        //     public long time;       // client-defined sort key (often timestamp)
        //   }
        String[][] fields = {
            // _id field -
            // { "id1", "long"},
            // { "link_type", "long"},
            // { "id2", "long"},
            { "visibility", "int"},
            { "data", "binData"},
            { "version", "int"},
            //{ "time", "long"}, -- does gte and lte
        };

        return generateSchema(keyId, fields);
    }


    private static String generateCountSchema(String keyId) {
        // Generate a JSON schema for the following collection.
        // Some fields are not marked as encrypted because they use query/update operators which
        // are not supported by FLE.
        // CREATE TABLE `counttable` (
        //     `id` bigint(20) unsigned NOT NULL DEFAULT '0',
        //     `link_type` bigint(20) unsigned NOT NULL DEFAULT '0',
        //     `count` int(10) unsigned NOT NULL DEFAULT '0',
        //     `time` bigint(20) unsigned NOT NULL DEFAULT '0',
        //     `version` bigint(20) unsigned NOT NULL DEFAULT '0',
        //     PRIMARY KEY (`id`,`link_type`)
        //   ) ENGINE=InnoDB DEFAULT CHARSET=latin1

        String[][] fields = {
            // _id
            { "id", "long"},
            { "link_type", "long"},
            // { "count ", "int"},  -- does inc
            { "time", "long"},
            // { "version", "int"}, -- does inc
        };

        return generateSchema(keyId, fields);
    }

    private static String generateSchema(String keyId, String[][] fields) {
        StringBuilder schema = new StringBuilder();

        schema.append(
            "{" +
            "  properties: {" );

            for(int i = 0; i < fields.length; i++) {
                schema.append(
                    "    " + fields[i][0] + ": {" +
                    "      encrypt: {" +
                    "        keyId: [{" +
                    "          \"$binary\": {" +
                    "            \"base64\": \"" + keyId + "\"," +
                    "            \"subType\": \"04\"" +
                    "          }" +
                    "        }]," +
                    "        bsonType: \"" + fields[i][1] + "\"," +
                    "        algorithm: \"AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic\"" +
                    "      }" +
                    "    },");
            }

        schema.append(
            "  }," +
            "  \"bsonType\": \"object\"" +
            "}");

        return schema.toString();
    }

    private static synchronized String getDataKeyOrCreate(MongoCollection<Document> keyCollection,ClientEncryption clientEncryption ) {
        BsonDocument findFilter = new BsonDocument();
        Document keyDoc = keyCollection.find(findFilter).first();

        String base64DataKeyId;
        if(keyDoc == null ) {
            BsonBinary dataKeyId = clientEncryption.createDataKey("local", new DataKeyOptions());
            base64DataKeyId = Base64.getEncoder().encodeToString(dataKeyId.getData());
        } else {
            UUID dataKeyId = (UUID) keyDoc.get("_id");
            base64DataKeyId = Base64.getEncoder().encodeToString(UuidUtils.asBytes(dataKeyId));
        }

        return base64DataKeyId;
    }

    private static AutoEncryptionSettings generateEncryptionSettings(String url) {
        // Use a hard coded local key since it needs to be shared between load and run phases
        byte[] localMasterKey = new byte[]{0x77, 0x1f, 0x2d, 0x7d, 0x76, 0x74, 0x39, 0x08, 0x50, 0x0b, 0x61, 0x14,
            0x3a, 0x07, 0x24, 0x7c, 0x37, 0x7b, 0x60, 0x0f, 0x09, 0x11, 0x23, 0x65,
            0x35, 0x01, 0x3a, 0x76, 0x5f, 0x3e, 0x4b, 0x6a, 0x65, 0x77, 0x21, 0x6d,
            0x34, 0x13, 0x24, 0x1b, 0x47, 0x73, 0x21, 0x5d, 0x56, 0x6a, 0x38, 0x30,
            0x6d, 0x5e, 0x79, 0x1b, 0x25, 0x4d, 0x2a, 0x00, 0x7c, 0x0b, 0x65, 0x1d,
            0x70, 0x22, 0x22, 0x61, 0x2e, 0x6a, 0x52, 0x46, 0x6a, 0x43, 0x43, 0x23,
            0x58, 0x21, 0x78, 0x59, 0x64, 0x35, 0x5c, 0x23, 0x00, 0x27, 0x43, 0x7d,
            0x50, 0x13, 0x65, 0x3c, 0x54, 0x1e, 0x74, 0x3c, 0x3b, 0x57, 0x21, 0x1a};

        Map<String, Map<String, Object>> kmsProviders =
            Collections.singletonMap("local", Collections.singletonMap("key", (Object)localMasterKey));

        // Use the same database, admin is slow
        String database = "linkdb0";
        String keyVaultCollection = "datakeys";
        String keyVaultNamespace = database + "." + keyVaultCollection;
        String keyVaultUrls = url;
        if (!keyVaultUrls.startsWith("mongodb")) {
            keyVaultUrls = "mongodb://" + keyVaultUrls;
        }

        MongoClientSettings keyVaultSettings = MongoClientSettings.builder()
        .applyConnectionString(new ConnectionString(keyVaultUrls))
        .build();

        ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder()
        .keyVaultMongoClientSettings(keyVaultSettings)
        .keyVaultNamespace(keyVaultNamespace)
        .kmsProviders(kmsProviders)
        .build();

        ClientEncryption clientEncryption = ClientEncryptions.create(clientEncryptionSettings);

        MongoClient vaultClient = new MongoClient( new MongoClientURI(keyVaultUrls) );

        final MongoCollection<Document> keyCollection = vaultClient.getDatabase(database).getCollection(keyVaultCollection);

        String base64DataKeyId = getDataKeyOrCreate(keyCollection, clientEncryption);

        AutoEncryptionSettings.Builder autoEncryptionSettingsBuilder = AutoEncryptionSettings.builder()
            .keyVaultNamespace(keyVaultNamespace)
            .extraOptions(Collections.singletonMap("mongocryptdBypassSpawn", (Object)true) )
            .kmsProviders(kmsProviders);

        Map<String, org.bson.BsonDocument> schemas = new HashMap<String, org.bson.BsonDocument>();
        System.out.println(generateNodeSchema(base64DataKeyId));
        schemas.put(database + ".nodetable", BsonDocument.parse(generateNodeSchema(base64DataKeyId)));
        schemas.put(database + ".linktable", BsonDocument.parse(generateLinkSchema(base64DataKeyId)));
        schemas.put(database + ".counttable", BsonDocument.parse(generateCountSchema(base64DataKeyId)));

        autoEncryptionSettingsBuilder.schemaMap(schemas);

        return autoEncryptionSettingsBuilder.build();
    }

    public void initialize(Properties props, Phase phase, int threadId) {
        // initialize the nodeIdGen value for the request phase, resetNodeStore will set the correct value for load
        // Retrieve names of database tables that will be used.
        counttable = ConfigUtil.getPropertyRequired(props, Config.COUNT_TABLE);
        if (counttable.equals("")) {
            String msg = "Error! " + Config.COUNT_TABLE + " is empty!"
                + "Please check configuration file.";
            logger.error(msg);
            throw new RuntimeException(msg);
        }

        nodetable = props.getProperty(Config.NODE_TABLE);
        if (nodetable.equals("")) {
            // For now, don't assume that nodetable is provided
            String msg = "Error! " + Config.NODE_TABLE + " is empty!"
                + "Please check configuration file.";
            logger.error(msg);
            throw new RuntimeException(msg);
        }

        linktable = props.getProperty(Config.LINK_TABLE);
        if (linktable.equals("")) {
            // For now, don't assume that linktable is provided
            String msg = "Error! " + Config.LINK_TABLE + " is empty!"
                + "Please check configuration file.";
            logger.error(msg);
            throw new RuntimeException(msg);
        }

        if (! props.containsKey(CONFIG_URL) && !props.containsKey(CONFIG_HOST)) {
            // url or host must be provided. If both are provided, then use url.
            throw new LinkBenchConfigError(String.format("Expected '%s' or '%s' configuration keys to be defined",
                    CONFIG_USER, CONFIG_HOST));
        }

        if (props.containsKey(CONFIG_URL)) {
            url = props.getProperty(CONFIG_URL);
        } else {
            host = ConfigUtil.getPropertyRequired(props, CONFIG_HOST);
            port = ConfigUtil.getInt(props, CONFIG_PORT, 27107);

            if (props.containsKey(CONFIG_USER) && !props.containsKey(CONFIG_PASSWORD) ||
                !props.containsKey(CONFIG_USER) && props.containsKey(CONFIG_PASSWORD)) {
                throw new LinkBenchConfigError(String.format("Both '%s' and '%s' must be supplied when credentials in use",
                        CONFIG_USER, CONFIG_HOST));
            }

            if (props.containsKey(CONFIG_USER)) {
                user = props.getProperty(CONFIG_USER);
                pwd = props.getProperty(CONFIG_PASSWORD).toCharArray();
            }
        }

        if (props.containsKey(SKIP_TRANSACTIONS)) {
            skip_transactions = ConfigUtil.getBool(props, SKIP_TRANSACTIONS);
        }

        bulkInsertSize = ConfigUtil.getInt(props, BULKINSERT_SIZE, DEFAULT_BULKINSERT_SIZE);
        max_retries = ConfigUtil.getInt(props, MAX_RETRIES, DEFAULT_MAX_RETRIES);

        Level debuglevel = ConfigUtil.getDebugLevel(props);
        debug = Level.DEBUG.isGreaterOrEqual(debuglevel);


        if (props.containsKey(FLE_ENABLE)) {
            use_encryption = ConfigUtil.getBool(props, FLE_ENABLE);

            if (use_encryption) {
                String dbprefix = ConfigUtil.getPropertyRequired(props, Config.DBPREFIX);
                int dbcount = ConfigUtil.getInt(props, Config.DBCOUNT, 1);

                if (!dbprefix.equals("linkdb") || dbcount != 1) {
                    throw new LinkBenchConfigError(String.format("FLE encryption is only supported when dbcount = 1 and dbprefix = 'linkdb', Actual: '%d' '%s'",
                            dbcount, dbprefix));
                }
            }
        }

        // Connect to database.
        try {
            openConnection();
        } catch (Exception e) {
            logger.error("error connecting to database:", e);
            throw e;
        }
    }

    /**
     * Open a connection to the remote mongo server.
     *
     * <ul>
     *     <li>current session is closed</li>
     *     <li>new connection is immediately validated by running the serverStatus command</li>
     *     <li>a new session is created</li>
     * </ul>:
     */
    private void openConnection() {
        if (session != null) {
            session.close();
            session = null;
        }

        // If a url is provided, it is used as is. If not, we add an option that requires the
        // MongoDB cluster to be a replicaset. Transactions can only be used on a mongod that has an
        // oplog. Note that if user didn't provide a url, the replSetName must literally be set to
        // "replset".

        // url="mongodb://localhost:27017/?readPreference=primary&replicaSet=replset"
        MongoClientOptions.Builder options = MongoClientOptions.builder();

        if (use_encryption) {
            AutoEncryptionSettings autoEncryptionSettings = generateEncryptionSettings(url);
            options.autoEncryptionSettings(autoEncryptionSettings);
        }

        // Open connection to the server.
        if (url != null) {
            mongoClient = new MongoClient(new MongoClientURI(url, options));
        } else {
            options.requiredReplicaSetName("replset");

            MongoCredential credentials = null;
            if(user != null) {
                credentials = MongoCredential.createCredential(user, "admin", pwd);
            }
            mongoClient = new MongoClient(new ServerAddress(host, port), credentials, options.build());
        }

        // Run a basic status command to make sure the connection is created and working properly.
        MongoDatabase adminDb = mongoClient.getDatabase("admin");
        if (!use_encryption) {
            adminDb.runCommand(new Document("serverStatus", 1));
        }

        session = mongoClient.startSession(ClientSessionOptions.builder().build());
    }

    @Override
    public void close() {
        if (session != null) {
            session.close();
            session = null;
        }

        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }

    @Override
    public void clearErrors(final int threadID) {
        logger.info("Closing and re-opening MongoDB client connection in threadID " + threadID);

        close();

        try {
            openConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Set of all error codes that indicate a transient MongoDB error
     * that should be handled by retrying.
     */
    private static final HashSet<Integer> retryMongoCodes = populateMongoRetryCodes();

    /**
     *  Populate retryMongoCodes.
     *  These error codes are defined in MongoDB source code:
     *  https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.err
     *
     *  TODO: SERVER-35141: 'Update the linkbench driver implementation to use TransientTransactionError label'
     */
    private static HashSet<Integer> populateMongoRetryCodes() {
        HashSet<Integer> states = new HashSet<>();
        states.add(24);     // LockTimeout
        states.add(46);     // LockBusy
        states.add(112);    // WriteConflict
        states.add(117);    // ConflictingOperationInProgress
        states.add(208);    // TooManyLocks
        states.add(225);    // TransactionTooOld
        states.add(226);    // AtomicityFailure
        states.add(239);    // SnapshotTooOld. TODO check if this is retryable
        states.add(246);    // SnapshotUnavailable. TODO check if this is retryable
        return states;
    }

    /**
     * Predicate that determines whether a MongoDB transaction error code is considered retryable.
     */
    private boolean isRetryableError(int errCode) {
        return retryMongoCodes.contains(errCode);
    }

    @Override
    public LinkWriteResult addLink(final String dbid, final Link link, final boolean noinverse) {
        logger.debug("addLink " + link.id1 +
            "." + link.id2 +
            "." + link.link_type);
        MongoDatabase database = mongoClient.getDatabase(dbid);
        final MongoCollection<Document> linkCollection = database.getCollection(linktable);
        final MongoCollection<Document> countCollection = database.getCollection(counttable);


        // Add link to the store.
        //    Update the the count table if visibility changes.
        //    Update link time, version, etc. in the case of a pre-existing link.
        CommandBlock<LinkWriteResult> block = new CommandBlock<LinkWriteResult>() {
            @Override
            public LinkWriteResult call() {
                final Bson linkId = linkBsonId(link);

                Bson projection = combine(include("visibility"), exclude("_id"));
                Bson update = combine(set("visibility", new BsonInt32(link.visibility)),
                        combine(
                                setOnInsert("_id", linkId),
                                setOnInsert("data", new BsonBinary(getNonEmptyData(link.data))),
                                setOnInsert("time", new BsonInt64(link.time)),
                                setOnInsert("version", new BsonInt32(link.version))));
                FindOneAndUpdateOptions options = new FindOneAndUpdateOptions().
                                                        upsert(true).
                                                        projection(projection).
                                                        returnDocument(ReturnDocument.BEFORE);
                Document preexisting = linkCollection.findOneAndUpdate(session, linkId, update, options);

                if ((preexisting == null  &&  link.visibility == VISIBILITY_DEFAULT) ||
                        (preexisting != null && preexisting.getInteger("visibility") != link.visibility)) {
                    final Bson filter = countBsonId(link.id1, link.link_type);

                    final long increment = link.visibility == VISIBILITY_DEFAULT ? 1L : -1L;
                    update = combine(
                            // TODO: SERVER-32442 check set _id and this functionality
                            set("_id", filter),
                            inc("count", increment),
                            set("time", new BsonInt64(link.time)),
                            inc("version", 1)
                    );
                    countCollection.updateOne(session, filter, update, new UpdateOptions().upsert(true));
                }

                if (preexisting != null) {
                    final Bson filter = linkBsonId(link);
                    update = combine(
                            set("data", new BsonBinary(getNonEmptyData(link.data))),
                            set("time", new BsonInt64(link.time)),
                            set("version", new BsonInt32(link.version)));

                    linkCollection.updateOne(session, filter, update);
                }
                if (check_count) {
                    testCount(dbid, linktable, counttable, link.id1, link.link_type);
                }
                if (preexisting == null) {
                  return LinkWriteResult.LINK_INSERT;
                } else {
                  // Ignore LINK_NO_CHANGE and LINK_NOT_DONE for now.
                  // 
                  // LINK_NO_CHANGE isn't required by Linkbench. It is possible to recognize updates that
                  // don't change the doc but without calling update directly that requires more CPU on
                  // the client. If the code above used updateOne then that returns matchedCount and
                  // modifiedCount and matched=1, modified=0 implies LINK_NO_CHANGE. Otherwise detection
                  // requires comparing the value of the possibly updated fields in "preexisting" with
                  // the fields in "link"
                  //
                  // LINK_NOT_DONE would only occur if this only tried to do update, not upsert.
                  return LinkWriteResult.LINK_UPDATE;
                }
            }

            @Override
            public String getName() {
                return "addLink";
            }
        };
        block = makeTransactional(block);
        block = makeRetryable(block);
        LinkWriteResult res = executeCommandBlock(block);
        return res;
    }

    @Override
    public boolean deleteLink(
            final String dbid,
            final long id1,
            final long link_type,
            final long id2,
            final boolean noinverse,
            final boolean expunge)
    {
        MongoDatabase database = mongoClient.getDatabase(dbid);
        final MongoCollection<Document> linkCollection = database.getCollection(linktable);
        final MongoCollection<Document> countCollection = database.getCollection(counttable);

        final BsonDocument linkId = linkBsonId(id1, link_type, id2);
        CommandBlock<Boolean> block = new CommandBlock<Boolean>() {

            @Override
            public Boolean call() {
                // Attempt to mark a link as hidden or completely expunge.
                // Update the count If a link was hidden or a visible link was delete.
                Bson projection = combine(include("visibility"), exclude("_id"));

                Document previous;
                if (!expunge) {
                    FindOneAndUpdateOptions options = new FindOneAndUpdateOptions().
                                                            projection(projection).
                                                            returnDocument(ReturnDocument.BEFORE);
                    previous = linkCollection.findOneAndUpdate(session,
                                                               linkId,
                                                               set("visibility", VISIBILITY_HIDDEN),
                                                               options);
                } else {
                     FindOneAndDeleteOptions options = new FindOneAndDeleteOptions().projection(projection);
                     previous = linkCollection.findOneAndDelete(session, linkId, options);
                }

                if (previous != null && previous.getInteger("visibility") == VISIBILITY_DEFAULT) {
                    decrementLinkCount(id1, link_type);
                }
                return previous != null;
            }

            private void decrementLinkCount(long id, long link_type) {
                BsonDocument countId = countBsonId(id, link_type);
                long currentTime = (new Date()).getTime();
                Bson update = combine(
                        setOnInsert("_id", countId),
                        setOnInsert("id", id),
                        setOnInsert("link_type", link_type),
                        inc("count", -1L),
                        set("time", new BsonInt64(currentTime)),
                        inc("version", 1L));
                final FindOneAndUpdateOptions options = new FindOneAndUpdateOptions().upsert(true);
                Document result = countCollection.findOneAndUpdate(session, countId, update, options);
                long count = result.getLong("count");
                if (count < -1L) {
                    throw new CommandBlockException("count less than -1: " + result);
                }
                if (check_count) {
                    testCount(dbid, linktable, counttable, id1, link_type);
                }
            }

            @Override
            public String getName() {
                return "deleteLink";
            }
        };
        block = makeTransactional(block);
        block = makeRetryable(block);
        return executeCommandBlock(block);
    }

    /**
     * This is method is for testing / validation purposes only. It is only ever called if the check_count property is
     * set to true. It does not run in a transaction by default but it should be called from within a transaction.
     *
     * For example
     *    $> ./bin/linkbench -Dcheck_count=true <OTHER PARAMS>
     *
     * The method compares the number of visible links matching id and link_type with the sum of the
     * counts matching id and link_type.
     *
     * @param dbid the database name
     * @param assoctable the link collection name
     * @param counttable the count collection name
     * @param id the value for the link collection id1 and count table id
     * @param link_type the type for the query
     */
    private void testCount(String dbid,
                           String assoctable, String counttable,
                           long id, long link_type) {
        MongoDatabase database = mongoClient.getDatabase(dbid);
        MongoCollection<Document> assocCollection = database.getCollection(assoctable);
        MongoCollection<Document> countCollection = database.getCollection(counttable);

        AggregateIterable<Document> iterable = assocCollection.aggregate(session, asList(
            match(combine(eq("id1",id),
                          eq("link_type", link_type),
                          eq("visibility", new BsonInt32(VISIBILITY_DEFAULT)))),
            group(null, Accumulators.sum("count", 1L))
        ));
        long count = 0L;
        Document result = iterable.first();
        if (result != null) {
            count = result.getLong("count");
        } else {
            logger.warn("testCount: null count");
        }

        iterable = countCollection.aggregate(session, asList(
                match(combine(eq("id",id), eq("link_type", link_type))),
                group(null,
                        Accumulators.sum("total", "$count"),
                        Accumulators.sum("instances", 1L))
        ));
        long total = 0;
        result = iterable.first();
        if (result != null) {
            total = result.getLong("total");
        } else {
            logger.warn("testCount: null total");
        }

        if (count != total) {
            throw new CommandBlockException("Data inconsistency between " + assoctable + "(" + count + ") " +
                    " and " + counttable + "(" + total + ")");
        }
    }

    @Override
    public LinkWriteResult updateLink(final String dbid, final Link link, final boolean noinverse) {
        return addLink(dbid, link, noinverse);
    }

    @Override
    public Link getLink(final String dbid, final long id1, final long link_type, final long id2) {
        MongoDatabase database = mongoClient.getDatabase(dbid);
        MongoCollection<Document> linkCollection = database.getCollection(linktable);

        Link link = null;
        BsonDocument linkId = linkBsonId(id1, link_type, id2);

        // Parse the retrieved link object and return it.
        // TODO: Consider implementing a custom codec for better performance or cleaner code.
        // But SERVER-32442 needs to be resolved first as the codec needs to be able to get or generate an
        // _id value OR we would need to derive a new Link class with an _id field (except for node which
        // has to use the id field or the value of the id field in _id).
        Document linkDoc = linkCollection.find(linkId).first();

        // Link not found.
        if (linkDoc != null) {
            link = linkFromBson(linkDoc);
        }
        return link;
    }

    @Override
    public Link[] getLinkList(final String dbid, final long id1, final long link_type) {
        return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, rangeLimit);
    }

    @Override
    public Link[] getLinkList(
        final String dbid,
        final long id1,
        final long link_type,
        final long minTimestamp,
        final long maxTimestamp,
        final int offset,
        final int limit)
    {
        final MongoDatabase database = mongoClient.getDatabase(dbid);
        final MongoCollection<Document> linkCollection = database.getCollection(linktable);

        // Find the links, limited by min/max timestamp and sorted by timestamp (descending).
        final Bson pred = and(
            eq("id1", id1),
            eq("link_type", link_type),
            gte("time", minTimestamp),
            lte("time", maxTimestamp),
            eq("visibility", new BsonInt32(VISIBILITY_DEFAULT)));

        CommandBlock<List> block = new CommandBlock<List>() {
            @Override
            public List<Link> call() {
                MongoCursor<Document> cursor = linkCollection
                    .find(pred)
                    .sort(new Document("time", -1))
                    .skip(offset)
                    .limit(limit).iterator();

                List<Link> links = new ArrayList<>();
                while (cursor.hasNext()) {
                    Document linkDoc = cursor.next();
                    links.add(linkFromBson(linkDoc));
                }

                return links;
            }

            @Override
            public String getName() {
                return "getLinkList";
            }
        };
        block = makeTransactional(block);
        block = makeRetryable(block);
        List<Link> links = executeCommandBlock(block);

        // Return array of links or null.
        Link[] linkArr = null;
        if (!links.isEmpty()) {
            linkArr = links.toArray(new Link[links.size()]);
        }
        return linkArr;
    }

    @Override
    public int bulkLoadBatchSize() {
        return bulkInsertSize;
    }

    @Override
    public void addBulkLinks(final String dbid, final List<Link> links, boolean noinverse) {
        CommandBlock<BulkWriteResult> block = new CommandBlock<BulkWriteResult>() {
            @Override
            public BulkWriteResult call() {
                if(links.isEmpty()) {
                    return null;
                }
                MongoDatabase database = mongoClient.getDatabase(dbid);
                final MongoCollection<Document> linkCollection = database.getCollection(linktable);
                // TODO: consider retryable
                final UpdateOptions options = new UpdateOptions().upsert(true);
                final List<WriteModel<Document>> operations = new LinkedList<>();

                for (Link link : links) {
                    final Bson filter = linkBsonId(link);
                    Bson update = combine(set("visibility", new BsonInt32(link.visibility)),
                            combine(
                                    setOnInsert("_id", filter),
                                    setOnInsert("data", new BsonBinary(getNonEmptyData(link.data))),
                                    setOnInsert("time", new BsonInt64(link.time)),
                                    setOnInsert("version", new BsonInt32(link.version))));

                    UpdateOneModel<Document> operation = new UpdateOneModel<>(filter, update, options);
                    operations.add(operation);
                }
                BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);
                BulkWriteResult result = linkCollection.bulkWrite(session, operations, bulkWriteOptions);
                int upserts = result.getUpserts().size();
                int matched = result.getMatchedCount();
                if (matched != links.size() && upserts != links.size()) {
                    throw new CommandBlockException("'bulkWrite' failed to bulk update all nodes properly.");
                }
                return result;
            }

            @Override
            public String getName() {
                return "addBulkLinks";
            }
        };
        block = makeTransactional(block);
        block = makeRetryable(block);
        BulkWriteResult res = executeCommandBlock(block);
        int nAdded = 0;
        if(res != null)
            nAdded = (res.getInsertedCount() + res.getUpserts().size());
        logger.trace("Added n=" + nAdded + " links");
    }

    /**
     * Add a batch of counts.
     *
     * @param dbid the database name.
     * @param counts a list of link count objects.
     */
    @Override
    public void addBulkCounts(String dbid, List<LinkCount> counts) {
        if(counts.isEmpty()) {
            logger.warn("addBulkCounts: counts=[] returning");
            return;
        }
        MongoDatabase database = mongoClient.getDatabase(dbid);
        final MongoCollection<Document> countCollection = database.getCollection(counttable);

        final List<WriteModel<Document>> operations = new ArrayList<>(counts.size());
        final UpdateOptions options = new UpdateOptions().upsert(true);
        for (LinkCount count : counts) {
            final Bson filter = countBsonId(count);
            // TODO: consider refactoring to common methods if sensible
            Bson update = combine(
                    setOnInsert("_id", filter),
                    set("count", new BsonInt64(count.count)),
                    set("version", new BsonInt64(count.version)),
                    set("time", new BsonInt64(count.time))
            );
            UpdateOneModel<Document> operation = new UpdateOneModel<>(filter, update, options);
            operations.add(operation);
        }
        CommandBlock<BulkWriteResult> block = new CommandBlock<BulkWriteResult>() {

            @Override
            public BulkWriteResult call() {
                BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);
                return countCollection.bulkWrite(session, operations, bulkWriteOptions);
            }
            public String getName() {
                return "addBulkCounts";
            }
        };
        block = makeTransactional(block);
        block = makeRetryable(block);
        executeCommandBlock(block);
    }

    @Override
    public long countLinks(final String dbid, final long id1, final long link_type) {
        MongoDatabase database = mongoClient.getDatabase(dbid);
        MongoCollection<Document> countCollection = database.getCollection(counttable);

        // Find the right count entry.
        Bson filter = combine(eq("id", id1), eq("link_type", link_type));
        Document document = countCollection.find(filter).first();
        if (document == null) {
            // No count entry exists.
            return 0;
        } else {
            return document.getLong("count");
        }

    }

    @Override
    public void resetNodeStore(final String dbid, final long startID) {
        // Remove all documents from the node store collection, and then reset the node id counter.
        MongoDatabase database = mongoClient.getDatabase(dbid);
        database.getCollection(nodetable).deleteMany(new Document());
        nodeIdGen = new AtomicLong(startID);
    }

    @Override
    public long addNode(final String dbid, final Node node) {
        long[] nodeIds = bulkAddNodes(dbid, Collections.singletonList(node));
        return nodeIds[0];
    }

    /**
     * Bulk load a  batch of nodes.
     *
     * NOTE: This method creates a new session with a new txnNumber per invocation. The session is closed
     * at the end of the method.
     *
     * @param dbid the database name.
     * @param nodes a list of node objects.
     */
    @Override
    public long[] bulkAddNodes(String dbid, final List<Node> nodes) {
        MongoDatabase database = mongoClient.getDatabase(dbid);
        final MongoCollection<Document> nodeCollection = database.getCollection(nodetable);
        final List<InsertOneModel<Document>> insertOps = new ArrayList<>();

        // MySql uses an auto incrementing id field so it avoids duplicate keys. The equivalent in MongoDb is to use an
        // ObjectId, but this is not possible as the base implementation generates random node ids based on the
        // configured range (startid1 to maxid1).
        //
        // In order to avoid duplicate key exceptions, nodeIdGen must be initialized with the last node id. The only
        // reliable way is to get the current max value. Something like ConcurrentMap.computeIfAbsent or
        // LongBinaryOperator would be a better way to do this but we are limited to Java 7.
        //
        // The following code may calculate nodeIdGen initial value multiple times at the start but it will only set it
        // once and there is an index to support a quick calculation.
        //
        if (nodeIdGen.get() == NODE_GEN_UNINITIALIZED) {
            logger.info("nodeIdGen initializing");
            Document document = nodeCollection.find(new Document())
                                 .projection(new Document("id", 1).append("_id", 0))
                                 .sort(Indexes.descending("id"))
                                 .limit(1)
                                 .first();
            long startId = 0;
            if (document != null ){
                startId = document.getLong("id") + 1;
            }
            if(nodeIdGen.compareAndSet(NODE_GEN_UNINITIALIZED, startId)) {
                logger.info("nodeIdGen set " + startId);
            }
        }

        // Create a list of node insert operations suitable for the transactions. We atomically pre-allocate a range
        // of node ids for the nodes we are going to insert. If the bulk node insertion fails, then these node ids will be
        // "missed" i.e. they will never be used again. We consider this acceptable, as long as, globally, node ids
        // always increase and no node id is ever assigned twice. This type of issue is similarly discussed in relation
        // to the MySQL "auto-increment" feature: https://dev.mysql.com/doc/refman/5.7/en/innodb-auto-increment-handling.html
        long nodeId = nodeIdGen.getAndAdd(nodes.size());
        final long[] assignedNodeIds = new long[nodes.size()];

        for (int i = 0; i < nodes.size(); i++) {
            Node node = nodes.get(i);
            assignedNodeIds[i] = nodeId;
            Document document = new Document()
                    .append("_id", new BsonInt64(nodeId))
                    .append("id", new BsonInt64(nodeId))
                    .append("type", new BsonInt32(node.type))
                    .append("version", new BsonInt64(node.version))
                    .append("time", new BsonInt32(node.time))
                    .append("data", new BsonBinary(getNonEmptyData(node.data)));

            insertOps.add(new InsertOneModel<>(document));
            nodeId += 1;
        }

        BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);
        BulkWriteResult res = nodeCollection.bulkWrite(session, insertOps, bulkWriteOptions);
        if (res.getInsertedCount() != nodes.size()) {
            throw new CommandBlockException("'bulkAddNodes' failed to bulk insert all nodes properly.");
        }
        return assignedNodeIds;
    }

    @Override
    public Node getNode(final String dbid, final int type, final long id) {
        final MongoDatabase database = mongoClient.getDatabase(dbid);
        final MongoCollection<Document> nodeCollection = database.getCollection(nodetable);

        // Lookup the node by id and type.
        final Bson query = and(eq("id", id), eq("type", type));

        // Fetch and parse the retrieved node object.
        // TODO: Consider implementing a custom codec for better performance or cleaner code.
        // But SERVER-32442 needs to be resolved first as the codec needs to be able to get or generate an
        // _id value OR we would need to derive a new Link class with an _id field (except for node which
        // has to use the id field or the value of the id field in _id).
        Document nodeDoc = nodeCollection.find(query).first();
        Node node = null;
        if (nodeDoc != null) {
            Binary bindata = (Binary) nodeDoc.get("data");
            byte data[] = bindata.getData();
            node = new Node(nodeDoc.getLong("id"),
                            nodeDoc.getInteger("type"),
                            nodeDoc.getLong("version"),
                            nodeDoc.getInteger("time"),
                            data);
        }
        return node;
    }

    @Override
    public boolean updateNode(final String dbid, final Node node) {
        final MongoDatabase database = mongoClient.getDatabase(dbid);
        final MongoCollection<Document> nodeCollection = database.getCollection(nodetable);

        final Bson nodeUpdate = combine(
            set("type", new BsonInt32(node.type)),
            set("version", new BsonInt64(node.version)),
            set("time", new BsonInt32(node.time)),
            set("data", new BsonBinary(getNonEmptyData(node.data))));

        // Update the node with the specified id.
        UpdateResult res = nodeCollection.updateOne(eq("id", node.id), nodeUpdate);

        // Node not found.
        if (res.getMatchedCount() == 0) {
            return false;
        }

        // Node was updated. Consider count != 1 an error.
        return res.getModifiedCount() == 1;
    }

    @Override
    public boolean deleteNode(final String dbid, final int type, final long id) {
        final MongoDatabase database = mongoClient.getDatabase(dbid);
        final MongoCollection<Document> nodeCollection = database.getCollection(nodetable);

        // Only delete the node if the 'id' and 'type' match.
        final Bson pred = and(eq("id", id), eq("type", type));

        DeleteResult res = nodeCollection.deleteOne(pred);

        // Return true if the node was deleted, else return false.
        return (res.getDeletedCount() == 1);
    }

    // TODO : SERVER-32442 revisit id v id1 v id2 confusion

    /**
     * Given a Link object, or the unique fields of a link, create a BSON object suitable for use as that link's
     * identifying field.
     */
    private BsonDocument linkBsonId(Link link) {
        return linkBsonId(link.id1, link.link_type, link.id2);
    }

    private BsonDocument linkBsonId(long id1, long link_type, long id2) {
        return new BsonDocument()
                .append("link_type", new BsonInt64(link_type))
                .append("id1", new BsonInt64(id1))
                .append("id2", new BsonInt64(id2));
    }

    /**
     * Given a Count object, or the unique fields of a count, create a BSON object suitable for use as that count's
     * identifying field.
     */
    private BsonDocument countBsonId(long id, long link_type) {
        return new BsonDocument()
                .append("id", new BsonInt64(id))
                .append("link_type", new BsonInt64(link_type));
    }

    private BsonDocument countBsonId(LinkCount count) {
        return countBsonId(count.id1, count.link_type);
    }

    /**
     * Given a document representing a Link, parse it into a Link object and return it.
     */
    private Link linkFromBson(Document linkDoc) {

        // Parse the retrieved link object.
        return new Link(
                linkDoc.getLong("id1"),
                linkDoc.getLong("link_type"),
                linkDoc.getLong("id2"),
                linkDoc.getInteger("visibility").byteValue(),
                ((Binary) linkDoc.get("data")).getData(),
                linkDoc.getInteger("version"),
                linkDoc.getLong("time"));
    }

    /**
     * Execute the block in a transaction.
     *
     * Start a new transaction and execute  task, if there is no exception, commit the transaction.
     *
     * @param task the task to execute.
     * @param <T> the return value type.
     * @return the return value from task.call()
     * @see #populateMongoRetryCodes for a list of retryable error codes
     */
    private <T> CommandBlock<T> makeTransactional(final CommandBlock<T> task) {
        if (skip_transactions)
            return task;
        return new CommandBlock<T>() {
            @Override
            public T call() {
                session.startTransaction();
                try {
                    T result = task.call();
                    session.commitTransaction();

                    if (debug) {
                        logger.debug(task.getName() + " returned " + result);
                    }
                    return result;
                } catch (MongoCommandException e) {
                    try {
                        session.abortTransaction();
                    } catch (IllegalStateException ise) {
                        logger.debug("abortTransaction failed with '" +ise.getMessage() + "', rethrowing original exception '" + e.getMessage() +"'");
                    }
                    throw e;
                }
            }
            @Override
            public String getName()  {
                return task.getName();
            }
        };
    }

    /**
     * Execute the mongo command in a retryable block.
     *
     * If there is a non-retryable exception reraise it.
     * If there is a retryable exception and and MAX_RETRIES is not exceeded, then retry.
     * If there is a retryable exception and and MAX_RETRIES is exceeded, reraise exception.
     *
     * @param task the task to execute.
     * @param <T> the return value type.
     * @return the return value from task.call()
     * @see #populateMongoRetryCodes for a list of retryable error codes
     */
    private <T> CommandBlock<T> makeRetryable(final CommandBlock<T> task) throws MongoCommandException, CommandBlockException {
        return new CommandBlock<T>() {
            @Override
            public T call() throws MongoCommandException, CommandBlockException {
                int retries = max_retries;
                while (true) {
                    retries --;
                    try {
                        return task.call();
                    } catch (MongoCommandException e) {
                        // If we failed due to a non-retryable error or max retries exceeded, rethrow the exception.
                        if (retries <= 0  || !isRetryableError(e.getCode())) {
                            logger.error(task.getName() + " failed after " + (max_retries - retries - 1) + " retries.", e);
                            throw e;
                        }
                        logger.debug(task.getName() + "("+ retries + ") retrying " + e.getMessage());
                    }
                }
            }

            @Override
            public String getName() {
                return task.getName();
            }
        };
    }

    /**
     * Execute the task.
     *
     * @param task the task to execute.
     * @param <T> the return value type.
     * @return the return value from task.call()
     *
     * @throws MongoCommandException reraise from driver
     * @throws CommandBlockException something happened in the command block
     */
    private <T> T executeCommandBlock(CommandBlock<T> task)  throws MongoCommandException, CommandBlockException {
        return task.call();
    }

    interface CommandBlock<T> {
        /**
         * Execute a block of mongo related commands. This block can be executed directly for no transactions or passed
         * to executeCommandBlock to handle transactions / retries and exceptions
         *
         * @return the end result of the block
         * @throws MongoCommandException if unable to compute a result
         * @throws CommandBlockException if there was an error (link bench driver should track these)
         * any other exception is likely a bug.
         */
        T call() throws MongoCommandException, CommandBlockException;

        String getName();
    }

    class CommandBlockException extends RuntimeException {

        /**
         * Constructs a new exception with the specified detail message.
         *
         * @param   message   the detail message. The detail message is saved for
         *          later retrieval by the {@link #getMessage()} method.
         */
        CommandBlockException(String message) {
            super(message);
        }

    }
}
