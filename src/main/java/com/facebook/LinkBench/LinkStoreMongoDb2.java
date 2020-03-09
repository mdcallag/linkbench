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
import java.util.concurrent.atomic.AtomicInteger;

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

/**
 * LinkStore implementation for MongoDB, using {@link ClientSession#startTransaction()},
 * {@link ClientSession#commitTransaction()} and {@link ClientSession#abortTransaction()}.
 *
 * @see com.mongodb.client.ClientSession
 */
public class LinkStoreMongoDb2 extends GraphStore {

    /* MongoDB database server configuration keys */
    static final String CONFIG_URL = "url";
    static final String CONFIG_HOST = "host";
    static final String CONFIG_PORT = "port";
    static final String CONFIG_USER = "user";
    static final String CONFIG_PASSWORD = "password";
    static final String MAX_RETRIES = "max_retries";
    static final String FLE_ENABLE = "fle_enable";

    // only valid for profiling the tests
    static final String SKIP_TRANSACTIONS = "skip_transactions";

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

    private final Logger logger = Logger.getLogger();
    private boolean debug = false; // true if debug log level is enabled

    LinkStoreMongoDb2() {
        super();
    }

    @SuppressWarnings("unused")
    LinkStoreMongoDb2(Properties props) {
        super();
        initialize(props, Phase.LOAD, 0);
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

        assert use_encryption == false : "not supported";

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
        logger.debug("addLink " + link.id1 + "." + link.id2 + "." + link.link_type);
        return newLink(dbid, link, noinverse, retry_add_link, max_add_link, "addLink", true);
    }

    @Override
    public LinkWriteResult updateLink(final String dbid, final Link link, final boolean noinverse) {
        logger.debug("updateLink " + link.id1 + "." + link.id2 + "." + link.link_type);
        return newLink(dbid, link, noinverse, retry_update_link, max_update_link, "updateLink", false);
    }

    private LinkWriteResult newLink(final String dbid, final Link link, final boolean noinverse,
                                    AtomicInteger retry_counter, AtomicInteger max_counter,
                                    final String caller, boolean addLink) {
        MongoDatabase database = mongoClient.getDatabase(dbid);
        final MongoCollection<Document> linkCollection = database.getCollection(linktable);
        final MongoCollection<Document> countCollection = database.getCollection(counttable);

        // This could be split into two functions. One that is a fast-path for insert and the
        // other for update. But the performance for the current approach looks good enough
        // and splitting these would be much more complex because some inserts will really
        // need to do an update and vice versa. See newLinkLoop in LinkStoreSQL to understand
        // the complexity.

        // Add link to the store.
        //    Update the the count table if visibility changes.
        //    Update link time, version, etc. in the case of a pre-existing link.
        CommandBlock<LinkWriteResult> block = new CommandBlock<LinkWriteResult>() {
            @Override
            public LinkWriteResult call() {
                final BsonBinary linkId = linkBsonId(link);
                final Bson idEq = eq("_id", linkId);

                Bson projection = combine(include("visibility"), exclude("_id"));
                Bson update = combine(
                                setOnInsert("_id", linkId),
                                setOnInsert("id1", new BsonInt64(link.id1)),
                                setOnInsert("link_type", new BsonInt64(link.link_type)),
                                setOnInsert("id2", new BsonInt64(link.id2)),
                                set("visibility", new BsonInt32(link.visibility)),
                                set("version", new BsonInt32(link.version)),
                                set("time", new BsonInt64(link.time)),
                                set("data", new BsonBinary(link.data)));

                FindOneAndUpdateOptions options = new FindOneAndUpdateOptions().
                                                        upsert(true).
                                                        projection(projection).
                                                        returnDocument(ReturnDocument.BEFORE);
                Document preexisting = linkCollection.findOneAndUpdate(session, idEq, update, options);

                if ((preexisting == null && link.visibility == VISIBILITY_DEFAULT) ||
                    (preexisting != null && preexisting.getInteger("visibility") != link.visibility)) {
                    final BsonBinary countId = countBsonId(link.id1, link.link_type);
                    final Bson countEq = eq("_id", countId);
                    long currentTime = (new Date()).getTime();
                    final long increment = link.visibility == VISIBILITY_DEFAULT ? 1L : -1L;
                    update = combine(
                            // TODO: SERVER-32442 check set _id and this functionality
                            setOnInsert("_id", countId),
                            setOnInsert("id", link.id1),
                            setOnInsert("link_type", link.link_type),
                            inc("count", increment),
                            set("time", new BsonInt64(currentTime)),
                            inc("version", 1)
                    );
                    countCollection.updateOne(session, countEq, update, new UpdateOptions().upsert(true));
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
        block = makeRetryable(block, new AtomicInteger[]{retry_counter, max_counter});
        LinkWriteResult res = executeCommandBlock(block);

        // Count the cases where addLink does an update and updateLink does an insert
        if (!addLink) {
            if (res == LinkWriteResult.LINK_INSERT)
                retry_upd_to_add.incrementAndGet();
        } else {
            if (res != LinkWriteResult.LINK_INSERT)
                retry_add_to_upd.incrementAndGet();
        }

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

        final BsonDocument idEq = new BsonDocument().append("_id", linkBsonId(id1, link_type, id2));

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
                                                               idEq,
                                                               set("visibility", VISIBILITY_HIDDEN),
                                                               options);
                } else {
                     FindOneAndDeleteOptions options = new FindOneAndDeleteOptions().projection(projection);
                     previous = linkCollection.findOneAndDelete(session, idEq, options);
                }

                if (previous != null && previous.getInteger("visibility") == VISIBILITY_DEFAULT) {
                    decrementLinkCount(id1, link_type);
                }
                return previous != null;
            }

            private void decrementLinkCount(long id, long link_type) {
                BsonBinary countId = countBsonId(id, link_type);
                Bson countEq = eq("_id", countId);
                long currentTime = (new Date()).getTime();
                Bson update = combine(
                        setOnInsert("_id", countId),
                        setOnInsert("id", id),
                        setOnInsert("link_type", link_type),
                        inc("count", -1L),
                        set("time", new BsonInt64(currentTime)),
                        inc("version", 1L));
                final FindOneAndUpdateOptions options = new FindOneAndUpdateOptions().upsert(true);
                Document result = countCollection.findOneAndUpdate(session, countEq, update, options);
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
        block = makeRetryable(block, new AtomicInteger[]{retry_delete_link, max_delete_link});
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
    public Link getLink(final String dbid, final long id1, final long link_type, final long id2) {
        MongoDatabase database = mongoClient.getDatabase(dbid);
        MongoCollection<Document> linkCollection = database.getCollection(linktable);

        Link link = null;

        // Parse the retrieved link object and return it.
        // TODO: Consider implementing a custom codec for better performance or cleaner code.
        // But SERVER-32442 needs to be resolved first as the codec needs to be able to get or generate an
        // _id value OR we would need to derive a new Link class with an _id field (except for node which
        // has to use the id field or the value of the id field in _id).
        Bson filter = eq("_id", linkBsonId(id1, link_type, id2));
        Document linkDoc = linkCollection.find(filter).first();

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

        // Query must be index-only on covering secondary index, don't fetch _id
        final Bson projDoc = combine(
                                include("id1"),
                                include("link_type"),
                                include("id2"),
                                include("visibility"),
                                include("data"),
                                include("version"),
                                include("time"),
                                exclude("_id"));

        CommandBlock<List> block = new CommandBlock<List>() {
            @Override
            public List<Link> call() {
                MongoCursor<Document> cursor = linkCollection
                    .find(pred)
                    .projection(projDoc)
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
        block = makeRetryable(block, new AtomicInteger[]{retry_get_link_list, max_get_link_list});
        List<Link> links = executeCommandBlock(block);

        // Return array of links or null.
        Link[] linkArr = null;
        if (!links.isEmpty()) {
            linkArr = links.toArray(new Link[links.size()]);
        }
        return linkArr;
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
                final List<WriteModel<Document>> operations = new LinkedList<>();

                for (Link link : links) {
                    final BsonBinary idBytes = linkBsonId(link);
                    Document insert_doc = new Document()
                        .append("_id", idBytes)
                        .append("id1", new BsonInt64(link.id1))
                        .append("link_type", new BsonInt64(link.link_type))
                        .append("id2", new BsonInt64(link.id2))
                        .append("visibility", new BsonInt32(link.visibility))
                        .append("version", new BsonInt32(link.version))
                        .append("time", new BsonInt64(link.time))
                        .append("data", new BsonBinary(link.data));

                    InsertOneModel<Document> operation = new InsertOneModel<>(insert_doc);
                    operations.add(operation);
                }
                BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);
                BulkWriteResult result = linkCollection.bulkWrite(session, operations, bulkWriteOptions);
                int inserts = result.getInsertedCount();
                if (inserts != links.size()) {
                    throw new CommandBlockException("'bulkWrite' inserted " + inserts + " of " +
                                                    links.size() + " Links");
                }
                return result;
            }

            @Override
            public String getName() {
                return "addBulkLinks";
            }
        };
        block = makeTransactional(block);
        block = makeRetryable(block, new AtomicInteger[]{retry_add_bulk_links, max_add_bulk_links});
        BulkWriteResult res = executeCommandBlock(block);
        int nAdded = 0;
        if(res != null)
            nAdded = res.getInsertedCount();
        logger.trace("Added n=" + nAdded + " links");
    }

    /**
     * Add a batch of counts.
     *
     * @param dbid the database name.
     * @param counts a list of link count objects.
     */
    @Override
    public void addBulkCounts(String dbid, final List<LinkCount> counts) {
        if(counts.isEmpty()) {
            logger.warn("addBulkCounts: counts=[] returning");
            return;
        }
        MongoDatabase database = mongoClient.getDatabase(dbid);
        final MongoCollection<Document> countCollection = database.getCollection(counttable);

        final List<WriteModel<Document>> operations = new ArrayList<>(counts.size());
        for (LinkCount count : counts) {
            final BsonBinary idBytes = countBsonId(count);
            Document insert_doc = new Document()
                    .append("_id", idBytes)
                    .append("id", new BsonInt64(count.id1))
                    .append("link_type", new BsonInt64(count.link_type))
                    .append("count", new BsonInt64(count.count))
                    .append("version", new BsonInt64(count.version))
                    .append("time", new BsonInt64(count.time));

            InsertOneModel<Document> operation = new InsertOneModel<>(insert_doc);
            operations.add(operation);
        }
        CommandBlock<BulkWriteResult> block = new CommandBlock<BulkWriteResult>() {

            @Override
            public BulkWriteResult call() {
                BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);
                BulkWriteResult result = countCollection.bulkWrite(session, operations, bulkWriteOptions);

                int inserts = result.getInsertedCount();
                if (inserts != counts.size()) {
                    throw new CommandBlockException("'bulkWrite' inserted " + inserts + " of " +
                                                    counts.size() + " Counts");
                }
                return result;
            }
            public String getName() {
                return "addBulkCounts";
            }
        };
        block = makeTransactional(block);
        block = makeRetryable(block, new AtomicInteger[]{retry_add_bulk_counts, max_add_bulk_counts});
        executeCommandBlock(block);
    }

    @Override
    public long countLinks(final String dbid, final long id1, final long link_type) {
        MongoDatabase database = mongoClient.getDatabase(dbid);
        MongoCollection<Document> countCollection = database.getCollection(counttable);

        // Find the right count entry.
        Bson filter = eq("_id", countBsonId(id1, link_type));
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
        // TODO: would be nice to have truncate command instead of deleteMany
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
        // TODO: count errors by error code
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
                                 .projection(new Document("_id", 1))
                                 .sort(Indexes.descending("_id"))
                                 .limit(1)
                                 .first();
            long startId = 0;
            if (document != null ){
                startId = document.getLong("_id") + 1;
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
                    .append("type", new BsonInt32(node.type))
                    .append("version", new BsonInt64(node.version))
                    .append("time", new BsonInt32(node.time))
                    .append("data", new BsonBinary(node.data));

            insertOps.add(new InsertOneModel<>(document));
            nodeId += 1;
        }

        CommandBlock<BulkWriteResult> block = new CommandBlock<BulkWriteResult>() {

            @Override
            public BulkWriteResult call() {
                BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);
                BulkWriteResult res = nodeCollection.bulkWrite(session, insertOps, bulkWriteOptions);
                if (res.getInsertedCount() != nodes.size()) {
                    throw new CommandBlockException("'bulkAddNodes' failed to bulk insert all nodes properly.");
                }
                return res;
            }

            public String getName() {
                return "bulkAddNodes";
            }
        };

        block = makeTransactional(block);
        block = makeRetryable(block, new AtomicInteger[]{retry_bulk_add_nodes, max_bulk_add_nodes});
        executeCommandBlock(block);

        return assignedNodeIds;
    }

    @Override
    public Node getNode(final String dbid, final int type, final long id) {
        final MongoDatabase database = mongoClient.getDatabase(dbid);
        final MongoCollection<Document> nodeCollection = database.getCollection(nodetable);

        // Lookup the node by id
        final Bson query = eq("_id", id);

        // Fetch and parse the retrieved node object.
        // TODO: see SERVER-32442 comment above
        Document nodeDoc = nodeCollection.find(query).first();
        if (nodeDoc == null)
            return null;

        Binary bindata = (Binary) nodeDoc.get("data");
        byte data[] = bindata.getData();
        Node node = new Node(nodeDoc.getLong("_id"),
                             nodeDoc.getInteger("type"),
                             nodeDoc.getLong("version"),
                             nodeDoc.getInteger("time"),
                             data);

        if (node.type != type) {
            logger.warn("getNode found id=" + id + " with wrong type (" + type + " vs " + node.type);
            return null;
        }

        return node;
    }

    @Override
    public boolean updateNode(final String dbid, final Node node) {
        // TODO: count errors by error code
        final MongoDatabase database = mongoClient.getDatabase(dbid);
        final MongoCollection<Document> nodeCollection = database.getCollection(nodetable);

        // Only update if the 'id' and 'type' match.
        final Bson pred = and(eq("_id", node.id), eq("type", node.type));

        final Bson nodeUpdate = combine(
            set("version", new BsonInt64(node.version)),
            set("time", new BsonInt32(node.time)),
            set("data", new BsonBinary(node.data)));

        // This can stop using a transaction when SERVER-44638 is fixed. Until then the
        // only way to get all writes to use j:false or j:true is to do all in a transaction.
        CommandBlock<UpdateResult> block = new CommandBlock<UpdateResult>() {

            @Override
            public UpdateResult call() {
                // Update the node with the specified id.
                UpdateResult res = nodeCollection.updateOne(pred, nodeUpdate);
                return res;
            }

            public String getName() {
                return "updateNode";
            }
        };

        block = makeTransactional(block);
        block = makeRetryable(block, new AtomicInteger[]{retry_update_node, max_update_node});
        UpdateResult res = executeCommandBlock(block);

        // Node not found.
        if (res.getMatchedCount() == 0) {
            return false;
        }

        // Node was updated. Consider count != 1 an error.
        return res.getModifiedCount() == 1;
    }

    @Override
    public boolean deleteNode(final String dbid, final int type, final long id) {
        // TODO: count errors by error code
        final MongoDatabase database = mongoClient.getDatabase(dbid);
        final MongoCollection<Document> nodeCollection = database.getCollection(nodetable);

        // Only delete the node if the 'id' and 'type' match.
        final Bson pred = and(eq("_id", id), eq("type", type));

        // This can stop using a transaction when SERVER-44638 is fixed. Until then the
        // only way to get all writes to use j:false or j:true is to do all in a transaction.
        CommandBlock<DeleteResult> block = new CommandBlock<DeleteResult>() {

            @Override
            public DeleteResult call() {
                return nodeCollection.deleteOne(pred);
            }

            public String getName() {
                return "deleteNode";
            }
        };

        block = makeTransactional(block);
        block = makeRetryable(block, new AtomicInteger[]{retry_delete_node, max_delete_node});
        DeleteResult res = executeCommandBlock(block);

        // Return true if the node was deleted, else return false.
        return (res.getDeletedCount() == 1);
    }

    /**
     * Given a Link object, or the unique fields of a link, create a 24-byte binary string by concatenating
     * bytes from the 3 fields to serve as the value of _id. This is safe for exact match but not for range
     * scan. If needed the encoding could be updated to be useful for range scan.
     */
    private BsonBinary linkBsonId(Link link) {
        return linkBsonId(link.id1, link.link_type, link.id2);
    }

    private BsonBinary linkBsonId(long id1, long link_type, long id2) {
        // TODO try allocateDirect
        ByteBuffer bb = ByteBuffer.allocate(24);
        bb.putLong(link_type);
        bb.putLong(id1);
        bb.putLong(id2);
        return new BsonBinary(bb.array());
    }

    /**
     * Given a Count object, or the unique fields of a count, create a 16-byte string by concatenating
     * bytes from the 2 fields to serve as the value of _id. See the comment above on range scan.
     */
    private BsonBinary countBsonId(long id, long link_type) {
        // TODO try allocateDirect
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(id);
        bb.putLong(link_type);
        return new BsonBinary(bb.array());
    }

    private BsonBinary countBsonId(LinkCount count) {
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
                    logger.warn("Commit failed for " + task.getName() + ": " + e.getMessage());
                    commit_failed.incrementAndGet();
                    try {
                        session.abortTransaction();
                    } catch (IllegalStateException ise) {
                        logger.warn("abortTransaction failed with '" + ise.getMessage() + "', rethrowing original exception '" + e.getMessage() +"'");
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
     * @param counters global counters for per operation retries
     * @param <T> the return value type.
     * @return the return value from task.call()
     * @see #populateMongoRetryCodes for a list of retryable error codes
     */
    private <T> CommandBlock<T> makeRetryable(final CommandBlock<T> task, final AtomicInteger[] counters)
        throws MongoCommandException, CommandBlockException {

        return new CommandBlock<T>() {
            @Override
            public T call() throws MongoCommandException, CommandBlockException {
                int retries = 0;
                while (true) {
                    try {
                        return task.call();
                    } catch (MongoCommandException e) {
                        retries++;
                        GraphStore.incError(e.getCode());

                        // If we failed due to a non-retryable error or max retries exceeded, rethrow the exception.
                        if (retries >= max_retries || !isRetryableError(e.getCode())) {
                            logger.error(task.getName() + " failed after " + max_retries + " retries.", e);
                            throw e;
                        }
                        logger.warn(task.getName() + "("+ retries + ") retrying " + e.getMessage());
                        counters[0].incrementAndGet();
                        // This has a race between get and set. It isn't worth preventing that race.
                        if (retries > counters[1].get())
                            counters[1].set(retries);
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
