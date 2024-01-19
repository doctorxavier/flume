package com.bcp.core.audit.flume.sink;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.bson.Document;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;

/**
 * User: guoqiang.li
 * Date: 12-9-12
 * Time: 13:31
 */
public class MongoSink extends AbstractSink implements Configurable {
	
	public static final int	MILLISECONDS	= 1000;
	public static final int	MINUTES			= 60;
	public static final int	SECONDS			= 60;
	public static final int	HOURS			= 24;
	
	public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String AUTHENTICATION_ENABLED = "authenticationEnabled";
    public static final String ACKNOWLEDGED_ENABLED = "acknowledgedEnabled";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String MODEL = "model";
    public static final String DB_NAME = "db";
    public static final String AUTHENTICATION_DB_NAME = "authenticationdb";
    public static final String COLLECTION = "collection";
    public static final String NAME_PREFIX = "MongSink_";
    public static final String BATCH_SIZE = "batch";
    public static final String BATCH_TIME = "batchTime";
    public static final String CONNECTIONS_PER_HOST = "connectionsPerHost";
    public static final String THREADS_ALLOWED_TO_BLOCK_FOR_CONNECTION_MULTIPLIER = "threadsAllowedToBlockForConnectionMultiplier";
    public static final String SOCKET_KEEP_ALIVE = "socketKeepAlive";
    public static final String THREADED = "threaded";
    public static final String MAX_THREADS = "maxThreads";
    public static final String AUTO_WRAP = "autoWrap";
    public static final String WRAP_FIELD = "wrapField";
    public static final String TIMESTAMP_FIELD = "timestampField";
    public static final String OPERATION = "op";
    public static final String PK = "_id";
    public static final String OP_INC = "$inc";
    public static final String OP_SET = "$set";
    public static final String OP_CURRENT_DATE = "$currentDate";
    public static final String DATE_FIELDS = "dateFields";
    public static final String LOG_FILE_PATH = "logFilePath";
    public static final String CXN_RESET_INTERVAL = "reset-connection-interval";
    public static final String MONITORING_THREADS = "monitoringThreads";
    public static final String MONITORING_THREADS_WARN_TOLERANCE = "monitoringThreadsWarnTolerance";
    public static final String MONITORING_THREADS_WARN_SEC_OPERATION = "monitoringThreadsWarnSecOperation";

    public static final String CHARSET = StandardCharsets.UTF_8.displayName();
    public static final boolean DEFAULT_AUTHENTICATION_ENABLED = false;
    public static final boolean DEFAULT_ACKNOWLEDGED_ENABLED = true;
    public static final boolean DEFAULT_THREADED = false;
    public static final int DEFAULT_MAX_THREADS = 100;
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 27017;
    public static final String DEFAULT_DB = "events";
    public static final String DEFAULT_COLLECTION = "events";
    public static final int DEFAULT_BATCH = 100;
    public static final long DEFAULT_BATCH_TIME = -1;
    public static final int DEFAULT_CONNECTIONS_PER_HOST = 100;
    public static final boolean DEFAULT_SOCKET_KEEP_ALIVE = false;
    public static final String DEFAULT_WRAP_FIELD = "log";
    public static final String DEFAULT_TIMESTAMP_FIELD = null;
    public static final char NAMESPACE_SEPARATOR = '.';
    public static final String OP_UPSERT = "upsert";
    public static final String OP_INSERT = "insert";
    public static final int DEFAULT_CXN_RESET_INTERVAL = 0;
    public static final boolean DEFAULT_MONITORING_THREADS = false;
    public static final int DEFAULT_MONITORING_THREADS_WARN_TOLERANCE = 65;
    public static final int DEFAULT_MONITORING_THREADS_WARN_SEC_OPERATION = 6;
    public static final int DEFAULT_THREADS_ALLOWED_TO_BLOCK_FOR_CONNECTION_MULTIPLIER = 5;

    public static final DateTimeParser[] PARSERS = {
	        DateTimeFormat.forPattern("yyyy-MM-dd").getParser(),
	        DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").getParser(),
	        DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").getParser(),
	        DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss Z").getParser(),
	        DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS Z").getParser(),
	        DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ").getParser(),
	        DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").getParser(),
	        DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssz").getParser(),
	        DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSz").getParser(),
	    };
    
    public static final DateTimeFormatter DATETIMEFORMATTER = new DateTimeFormatterBuilder().append(null, PARSERS).toFormatter();
	
	private static final Logger LOGGER = LoggerFactory.getLogger(MongoSink.class);
    
    private static final Boolean DEFAULT_AUTO_WRAP = false;
    
    private static AtomicInteger counter = new AtomicInteger();
	
	protected MongoClient mongoClient;
    protected boolean authenticationEnabled;
    protected boolean acknowledgedEnabled;
    protected String username;
    protected String password;
    protected String collectionName;
    protected int batchSize;

    private SinkCounter sinkCounter;
    private AtomicBoolean resetConnectionFlag;
    private int cxnResetInterval;
	private final ScheduledExecutorService cxnResetExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Mongo Sink Reset Thread").build());
    
    private int connectionsPerHost;
    private int threadsAllowedToBlockForConnectionMultiplier;
    private boolean socketKeepAlive;
    private boolean threaded;
    private int maxThreads;
    private boolean monitoringThreads;
    private int monitoringThreadsWarnTolerance;
    private int monitoringThreadsWarnSecOperation;
    
    private String host;
    private List<ServerAddress> addresses;
    private int port;
    private CollectionModel model;
    private String dbName;
    private String authenticationdbName;
    private long batchTime;
    private boolean autoWrap;
    private String wrapField;
    private String timestampField;
    private String[] ids = {"_id"};
    private Set<String> dateFields = new HashSet<String>(0);
    private String operation;
    private String logFilePath;
    
    private Map<UUID, Map<String, List<Document>>> eventMaps = new ConcurrentHashMap<UUID, Map<String, List<Document>>>();
    private Map<UUID, Map<String, List<Document>>> upsertMaps = new ConcurrentHashMap<UUID, Map<String, List<Document>>>();
    
    private WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;
    
    private Writer logFile;
    
    private MongoClientOptions options;

    public void configure(Context context) {
		if (getName() == null) {
			setName(NAME_PREFIX + counter.getAndIncrement());
		}
    	
		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}
		cxnResetInterval = context.getInteger(CXN_RESET_INTERVAL, DEFAULT_CXN_RESET_INTERVAL);
		if (cxnResetInterval == DEFAULT_CXN_RESET_INTERVAL) {
			LOGGER.info("Connection reset is set to " + String.valueOf(DEFAULT_CXN_RESET_INTERVAL) + ". Will not reset connection to next hop");
		}

        host = context.getString(HOST, DEFAULT_HOST);
        port = context.getInteger(PORT, DEFAULT_PORT);
        authenticationEnabled = context.getBoolean(AUTHENTICATION_ENABLED, DEFAULT_AUTHENTICATION_ENABLED);
        acknowledgedEnabled = context.getBoolean(ACKNOWLEDGED_ENABLED, DEFAULT_ACKNOWLEDGED_ENABLED);

		if (!acknowledgedEnabled) {
			writeConcern = WriteConcern.UNACKNOWLEDGED;
		}
		if (authenticationEnabled) {
			username = context.getString(USERNAME);
			password = context.getString(PASSWORD);
		} else {
			username = "";
			password = "";
		}
        
        model = CollectionModel.valueOf(context.getString(MODEL, CollectionModel.single.name()));
        dbName = context.getString(DB_NAME, DEFAULT_DB);
        authenticationdbName = context.getString(AUTHENTICATION_DB_NAME, DEFAULT_DB);
        collectionName = context.getString(COLLECTION, DEFAULT_COLLECTION);
        batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH);
        autoWrap = context.getBoolean(AUTO_WRAP, DEFAULT_AUTO_WRAP);
        wrapField = context.getString(WRAP_FIELD, DEFAULT_WRAP_FIELD);
        timestampField = context.getString(TIMESTAMP_FIELD, DEFAULT_TIMESTAMP_FIELD);
        batchTime = context.getLong(BATCH_TIME, DEFAULT_BATCH_TIME);
        operation = context.getString(OPERATION, "");
        connectionsPerHost = context.getInteger(CONNECTIONS_PER_HOST, DEFAULT_CONNECTIONS_PER_HOST);
        threadsAllowedToBlockForConnectionMultiplier = context.getInteger(THREADS_ALLOWED_TO_BLOCK_FOR_CONNECTION_MULTIPLIER, DEFAULT_THREADS_ALLOWED_TO_BLOCK_FOR_CONNECTION_MULTIPLIER);
        socketKeepAlive = context.getBoolean(SOCKET_KEEP_ALIVE, DEFAULT_SOCKET_KEEP_ALIVE);
        threaded = context.getBoolean(THREADED, DEFAULT_THREADED);
        maxThreads = context.getInteger(MAX_THREADS, DEFAULT_MAX_THREADS);
        monitoringThreads = context.getBoolean(MONITORING_THREADS, DEFAULT_MONITORING_THREADS);
        monitoringThreadsWarnTolerance = context.getInteger(MONITORING_THREADS_WARN_TOLERANCE, DEFAULT_MONITORING_THREADS_WARN_TOLERANCE);
        monitoringThreadsWarnSecOperation = context.getInteger(MONITORING_THREADS_WARN_SEC_OPERATION, DEFAULT_MONITORING_THREADS_WARN_SEC_OPERATION);
        
		String ids = context.getString(PK);
		if (ids != null) {
			this.ids = ids.split(" ");
		}

		String dateFields = context.getString(DATE_FIELDS);
		if (dateFields != null) {
			this.dateFields.addAll(Lists.newArrayList(dateFields.split(" ")));
		}

		logFilePath = context.getString(LOG_FILE_PATH);

		if (logFilePath != null) {
			try {
				logFile = new BufferedWriter(new FileWriter(logFilePath, true));
			} catch (IOException e) {
				LOGGER.error(ExceptionUtils.getMessage(e));
			}
		}

		this.options = MongoClientOptions.builder()
        		.socketKeepAlive(socketKeepAlive)
                .connectionsPerHost(connectionsPerHost)
                .threadsAllowedToBlockForConnectionMultiplier(threadsAllowedToBlockForConnectionMultiplier)
                .writeConcern(writeConcern)
                .build();
		
		final String[] hosts = host.split(",");
		if (hosts.length > 0) {
			addresses = new ArrayList<ServerAddress>();
			for (int i = 0; i < hosts.length; i++) {
				addresses.add(new ServerAddress(hosts[i]));
			}
		}
        
        LOGGER.info("MongoSink {} context { host:{}, port:{}, authentication_enabled:{}, username:{}, password:{}, model:{}, authenticationdbName:{}, dbName:{}, collectionName:{}, batch: {}, autoWrap: {}, wrapField: {}, timestampField: {}, operation: {}, connectionsPerHost: {}, threadsAllowedToBlockForConnectionMultiplier: {}, socketKeepAlive: {}, acknowledgedEnabled: {}, threaded: {}, maxThreads: {}, monitoringThreads: {}, monitoringThreadsWarnTolerance: {}, monitoringThreadsWarnSecOperation: {}, logFilePath: {}, ids: {}, dateFields: {} }",
                new Object[]{getName(), host, port, authenticationEnabled, username, password, model, authenticationdbName, dbName, collectionName, batchSize, autoWrap, wrapField, timestampField, operation, connectionsPerHost, threadsAllowedToBlockForConnectionMultiplier, socketKeepAlive, acknowledgedEnabled, threaded, maxThreads, monitoringThreads, monitoringThreadsWarnTolerance, monitoringThreadsWarnSecOperation, logFilePath, ids, dateFields});
    }
    
	private MongoClient initializeMongoClient() {
		MongoClient mongoClient;
		if (!authenticationEnabled) {
			try {
				if (addresses == null) {
					mongoClient = new MongoClient(new ServerAddress(host, port), options);
				} else {
					mongoClient = new MongoClient(addresses, options);
				}
			} catch (IllegalArgumentException e) {
				LOGGER.error("Can't connect to mongoDB", e);
				return null;
			}
		} else {
			try {
				final MongoCredential mongoCredential = MongoCredential.createScramSha1Credential(username, authenticationdbName, password.toCharArray());
				if (addresses == null) {
					mongoClient = new MongoClient(new ServerAddress(host, port), Arrays.asList(mongoCredential), options);
				} else {
					mongoClient = new MongoClient(addresses, Arrays.asList(mongoCredential), options);
				}
			} catch (IllegalArgumentException e) {
				LOGGER.error("Can't connect to mongoDB", e);
				return null;
			} catch (MongoException e) {
				LOGGER.error("CRITICAL FAILURE: Unable to authenticate. Check username and Password, or use another unauthenticated DB. Not starting MongoDB sink.\n");
				return null;
			}
		}
		return mongoClient;
	}
    
	private void createConnection() throws FlumeException {
		if (mongoClient == null) {
			LOGGER.info("Mongo sink {}: Building MongoClient with hostname: {}, port: {}", new Object[] {getName(), host, port});
			try {
				resetConnectionFlag = new AtomicBoolean(false);
				mongoClient = initializeMongoClient();
				Preconditions.checkNotNull(mongoClient, "Mongo Client could not be initialized. " + getName() + " could not be started");
				sinkCounter.incrementConnectionCreatedCount();
				if (cxnResetInterval > 0) {
					cxnResetExecutor.schedule(new Runnable() {

						public void run() {
							resetConnectionFlag.set(true);
						}
					}, cxnResetInterval, TimeUnit.SECONDS);
				}
			} catch (Exception ex) {
				sinkCounter.incrementConnectionFailedCount();
				if (ex instanceof FlumeException) {
					throw (FlumeException) ex;
				} else {
					throw new FlumeException(ex);
				}
			}
			LOGGER.debug("Mongo sink {}: Created MongoClient: {}", getName(), mongoClient);
		}

	}
    
	private void destroyConnection() {
		if (mongoClient != null) {
			LOGGER.debug("Mongo sink {} closing Mongo client: {}", getName(), mongoClient);
			try {
				mongoClient.close();
				sinkCounter.incrementConnectionClosedCount();
			} catch (FlumeException e) {
				sinkCounter.incrementConnectionFailedCount();
				LOGGER.error("Mongo sink " + getName() + ": Attempt to close Mongo client failed. Exception follows.", e);
			}
		}
		mongoClient = null;
	}
	
	private void verifyConnection() throws FlumeException {
		if (mongoClient == null) {
			createConnection();
		} else if (mongoClient.getAddress() == null) {
			destroyConnection();
			createConnection();
		}
	}

	private void resetConnection() {
		try {
			destroyConnection();
			createConnection();
		} catch (Throwable throwable) {
			// Don't rethrow, else this runnable won't get scheduled again.
			LOGGER.error("Error while trying to expire connection", throwable);
		}
	}

	@Override
	public synchronized void start() {
		LOGGER.info("Starting {}...", this);
		sinkCounter.start();
		try {
			createConnection();
		} catch (FlumeException e) {
			LOGGER.warn("Unable to create Mongo client using hostname: " + host + ", port: " + port, e);

			/* Try to prevent leaking resources. */
			destroyConnection();
		}

		super.start();

		LOGGER.info("Mongo sink {} started.", getName());
	}
    
	@Override
	public synchronized void stop() {

		LOGGER.info("Mongo sink {} stopping...", getName());

		while (!(this.eventMaps.isEmpty() && this.upsertMaps.isEmpty())) {
			try {

				printEventMaps(getName(), this.eventMaps, OP_INSERT, true);
				printEventMaps(getName(), this.upsertMaps, OP_UPSERT, true);

				Thread.sleep(MILLISECONDS);
			} catch (InterruptedException | UnsupportedEncodingException e) {
				LOGGER.error(ExceptionUtils.getMessage(e));
			}
		}

		if (logFile != null) {
			try {
				logFile.close();
			} catch (IOException e) {
				LOGGER.error(ExceptionUtils.getMessage(e));
			}
		}

		destroyConnection();
		cxnResetExecutor.shutdown();
		try {
			if (cxnResetExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
				cxnResetExecutor.shutdownNow();
			}
		} catch (Exception ex) {
			LOGGER.error("Interrupted while waiting for connection reset executor to shut down");
		}

		sinkCounter.stop();
		super.stop();

		LOGGER.info("Mongo sink {} stopped. Metrics: {}", getName(), sinkCounter);
	}

    public Status process() throws EventDeliveryException {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("{} start to process event", getName());
		}
		Status status = Status.READY;
		try {
			status = parseEvents();
		} catch (Exception e) {
			LOGGER.error("can't process events", e);
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("{} processed events", getName());
		}
		return status;
    }

    protected void saveEvents(Map<String, List<Document>> eventMap) {
		if (eventMap.isEmpty()) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("eventMap is empty");
			}
			return;
		}

		for (Map.Entry<String, List<Document>> entry : eventMap.entrySet()) {
			final List<Document> docs = entry.getValue();
			final String eventCollection = entry.getKey();
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("collection: {}, length: {}", eventCollection, docs.size());
			}
			final int separatorIndex = eventCollection.indexOf(NAMESPACE_SEPARATOR);
			String eventDb = eventCollection.substring(0, separatorIndex);
			String collectionName = eventCollection.substring(separatorIndex + 1);

			MongoDatabase db = mongoClient.getDatabase(eventDb);

			try {
				db.getCollection(collectionName).insertMany(docs);
			} catch (MongoException e) {
				LOGGER.error("can't insert documents with error: {} , {}", e.getCode(), e.getMessage());
			}

		}
    }
    
    private Status parseProcessEvent(Map<String, List<Document>> eventMap, Map<String, List<Document>> upsertMap, Event event) throws UnsupportedEncodingException {
		Status status = Status.READY;
		if (event == null) {
			status = Status.BACKOFF;
			return status;
		} else {
			String operation = event.getHeaders().get(OPERATION);
			if (StringUtils.isEmpty(operation)) {
				operation = this.operation;
			}
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("event operation is {}", operation);
			}
			if (OP_UPSERT.equalsIgnoreCase(operation)) {
				processEvent(upsertMap, event);
			} else if (StringUtils.isNotBlank(operation)) {
				LOGGER.error("non-supports operation {}", operation);
			} else {
				processEvent(eventMap, event);
			}
		}
		return status;
	}

	private Status parseEvents() throws EventDeliveryException {
		Status status = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();

		Map<String, List<Document>> eventMap = new HashMap<String, List<Document>>();
		Map<String, List<Document>> upsertMap = new HashMap<String, List<Document>>();
		try {
			
			if (resetConnectionFlag.get()) {
				resetConnection();
				// if the time to reset is long and the timeout is short
				// this may cancel the next reset request
				// this should however not be an issue
				resetConnectionFlag.set(false);
			}

			transaction.begin();

			verifyConnection();

			long currentTimeMillis = -1;
			long elapsedTimeMillis = -1;

			if (batchTime > 0) {
				if (LOGGER.isTraceEnabled()) {
					LOGGER.trace("ParseEvents, batchTime: " + batchTime);
				}
				currentTimeMillis = System.currentTimeMillis();
				long batchTimeMillis = System.currentTimeMillis() + batchTime;
				while (currentTimeMillis < batchTimeMillis) {
					Event event = channel.take();
					status = parseProcessEvent(eventMap, upsertMap, event);
					currentTimeMillis = System.currentTimeMillis();
				}
			} else {
				if (LOGGER.isTraceEnabled()) {
					LOGGER.trace("ParseEvents, batchSize: " + batchSize);
					currentTimeMillis = System.currentTimeMillis();
				}
				for (int i = 0; i < batchSize; i++) {
					if (LOGGER.isTraceEnabled()) {
						elapsedTimeMillis = System.currentTimeMillis() - currentTimeMillis;
						LOGGER.trace("ParseEvents channel.pre-take, Elapsed Millis: " + elapsedTimeMillis);
					}
					Event event = channel.take();
					if (LOGGER.isTraceEnabled()) {
						elapsedTimeMillis = System.currentTimeMillis() - currentTimeMillis;
						LOGGER.trace("ParseEvents channel.take, Elapsed Millis: " + elapsedTimeMillis);
					}
					status = parseProcessEvent(eventMap, upsertMap, event);
					if (LOGGER.isTraceEnabled()) {
						elapsedTimeMillis = System.currentTimeMillis() - currentTimeMillis;
						LOGGER.trace("ParseEvents parseProcessEvent, Elapsed Millis: " + elapsedTimeMillis);
						LOGGER.trace("Event is null: " + (event == null) + ", status is BACKOFF: " + (status == Status.BACKOFF));
					}
					if (status == Status.BACKOFF) {
						break;
					}
				}
			}
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("EventMap.isEmpty: " + eventMap.isEmpty());
				LOGGER.trace("UpsertMap.isEmpty: " + upsertMap.isEmpty());
				currentTimeMillis = System.currentTimeMillis();
			}

			int size = 0;
			for (Map.Entry<String, List<Document>> entry : eventMap.entrySet()) {
				size += entry.getValue().size();
			}

			for (Map.Entry<String, List<Document>> entry : upsertMap.entrySet()) {
				size += entry.getValue().size();
			}

			if (size == 0) {
				sinkCounter.incrementBatchEmptyCount();
				status = Status.BACKOFF;
			} else {
				if (size < batchSize) {
					sinkCounter.incrementBatchUnderflowCount();
				} else {
					sinkCounter.incrementBatchCompleteCount();
				}
				sinkCounter.addToEventDrainAttemptCount(size);
			}
			
			if (!eventMap.isEmpty()) {
				if (threaded) {
					UUID uuid = UUID.randomUUID();
					eventMaps.put(uuid, eventMap);
					MongoSinkThread mongoSinkThread = new MongoSinkThread(getName(), uuid, eventMaps, OP_INSERT, eventMaps.size());
					new Thread(mongoSinkThread).start();
				} else {
					saveEvents(eventMap);
					if (logFilePath != null) {
						for (Map.Entry<String, List<Document>> entry : eventMap.entrySet()) {
							for (Document dbObject : entry.getValue()) {
								logFile.append(dbObject.toString() + "\n");
								logFile.flush();
							}
						}
					}
				}
			}
			
			if (!upsertMap.isEmpty()) {
				if (threaded) {
					UUID uuid = UUID.randomUUID();
					upsertMaps.put(uuid, upsertMap);
					MongoSinkThread mongoSinkThread = new MongoSinkThread(getName(), uuid, upsertMaps, OP_UPSERT, upsertMaps.size());
					new Thread(mongoSinkThread).start();
				} else {
					doUpsert(upsertMap);
					if (logFilePath != null) {
						for (Map.Entry<String, List<Document>> entry : upsertMap.entrySet()) {
							for (Document dbObject : entry.getValue()) {
								logFile.append(dbObject.toString() + "\n");
								logFile.flush();
							}
						}
					}
				}
			}

			transaction.commit();
			
			if (!threaded) {
				sinkCounter.addToEventDrainSuccessCount(size);
			}
			
			if (LOGGER.isTraceEnabled()) {
				elapsedTimeMillis = System.currentTimeMillis() - currentTimeMillis;
				LOGGER.trace("ParseEvents finished, Elapsed Millis: " + elapsedTimeMillis);
			}
			
		} catch (Throwable t) {
			transaction.rollback();
			if (t instanceof Error) {
				throw (Error) t;
			} else if (t instanceof ChannelException) {
				LOGGER.error("Mongo Sink " + getName() + ": Unable to get event from channel " + channel.getName() + ". Exception follows.", t);
				status = Status.BACKOFF;
			} else {
				destroyConnection();
				throw new EventDeliveryException("Failed to send events", t);
			}
		} finally {
			transaction.close();
		}
		return status;
	}

	protected void doUpsert(Map<String, List<Document>> eventMap) {
		if (eventMap.isEmpty()) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("eventMap is empty");
			}
			return;
        }

		for (Map.Entry<String, List<Document>> entry : eventMap.entrySet()) {
			final String eventCollection = entry.getKey();
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("EventCollection: " + eventCollection);
			}
			List<Document> docs = entry.getValue();
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("collection: {}, length: {}", eventCollection, docs.size());
			}
			int separatorIndex = eventCollection.indexOf(NAMESPACE_SEPARATOR);
			String eventDb = eventCollection.substring(0, separatorIndex);
			String collectionName = eventCollection.substring(separatorIndex + 1);

            MongoDatabase db = mongoClient.getDatabase(eventDb);

			MongoCollection<Document> collection = db.getCollection(collectionName);
			for (final Document doc : docs) {

				final Document filter = new Document();
				Document update = doc;

				for (String id : ids) {
					filter.append(id, doc.get(id));

					if (dateFields.contains(id)) {
						Date timestamp = new Date();
						if (doc.containsKey(id)) {
							Object object = doc.get(id);
							if (object instanceof String) {
								String dateText = (String) object;
								try {
									timestamp = DATETIMEFORMATTER.parseDateTime(dateText).toDate();
									filter.append(id, timestamp);
								} catch (Exception e) {
									LOGGER.error("can't parse date " + dateText, e);
								}
							}
						}
						filter.append(id, timestamp);
					}
				}
            	
            	boolean isOpInc = doc.keySet().contains(OP_INC);
                boolean isOpSet = doc.keySet().contains(OP_SET);
                boolean isOpCurrentDate = doc.keySet().contains(OP_CURRENT_DATE);
                
				if (isOpInc || isOpSet || isOpCurrentDate) {
					update = new Document();
					if (isOpInc) {
						update.append(OP_INC, doc.get(OP_INC));
					}
					if (isOpSet) {
						Object objOpSet = doc.get(OP_SET);
						if (objOpSet instanceof Document) {
							Document opSet = (Document) objOpSet;

							for (String dateField : dateFields) {
								Date timestamp = new Date();

								if (opSet.containsKey(dateField)) {
									Object object = opSet.get(dateField);
									if (object instanceof String) {
										String dateText = (String) object;
										try {
											timestamp = DATETIMEFORMATTER.parseDateTime(dateText).toDate();
											opSet.remove(dateField);
											opSet.put(dateField, timestamp);
										} catch (Exception e) {
											LOGGER.error("can't parse date " + dateText, e);
										}
									}
								}
								opSet.put(dateField, timestamp);
							}
							update.append(OP_SET, doc.get(OP_SET));
						}
					}
					if (isOpCurrentDate) {
						update.append(OP_CURRENT_DATE, doc.get(OP_CURRENT_DATE));
					}
				}
            	
				if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("filter: {}, update: {}", filter, update);
                }
				try {
					collection.updateMany(filter, update, new UpdateOptions().upsert(true));
				} catch (MongoException e) {
					LOGGER.error("can't upsert documents with error: {} , {}", e.getCode(), e.getMessage());
				}
            }
        }
    }

    private void processEvent(Map<String, List<Document>> eventMap, Event event) throws UnsupportedEncodingException {
        switch (model) {
            case single:
                putSingleEvent(eventMap, event);

                break;
            case dynamic:
                putDynamicEvent(eventMap, event);

                break;
            default:
                LOGGER.error("can't support model: {}, please check configuration.", model);
        }
    }

	private void putDynamicEvent(Map<String, List<Document>> eventMap, Event event) throws UnsupportedEncodingException {
		String eventCollection;
		Map<String, String> headers = event.getHeaders();
		String eventDb = headers.get(DB_NAME);
		eventCollection = headers.get(COLLECTION);

		if (!StringUtils.isEmpty(eventDb)) {
			eventCollection = eventDb + NAMESPACE_SEPARATOR + eventCollection;
		} else {
			eventCollection = dbName + NAMESPACE_SEPARATOR + eventCollection;
		}

		if (!eventMap.containsKey(eventCollection)) {
			eventMap.put(eventCollection, new ArrayList<Document>());
		}

		List<Document> documents = eventMap.get(eventCollection);
		if (documents == null) {
			documents = new ArrayList<Document>(batchSize);
		}
		addEventToList(documents, event);
	}

	private void putSingleEvent(Map<String, List<Document>> eventMap, Event event) throws UnsupportedEncodingException {
		String eventCollection;
		eventCollection = dbName + NAMESPACE_SEPARATOR + collectionName;
		if (!eventMap.containsKey(eventCollection)) {
			eventMap.put(eventCollection, new ArrayList<Document>());
		}

		List<Document> documents = eventMap.get(eventCollection);
		if (documents == null) {
			documents = new ArrayList<Document>(batchSize);
		}
		addEventToList(documents, event);
	}

	protected List<Document> addEventToList(List<Document> documents, Event event) throws UnsupportedEncodingException {
		Document eventJson;
        byte[] body = event.getBody();
        if (autoWrap) {
            eventJson = new Document(wrapField, new String(body, CHARSET));
        } else {
            try {
                eventJson = Document.parse(new String(body, CHARSET));
            } catch (Exception e) {
                LOGGER.error("Can't parse events: " + new String(body, CHARSET), e);
                return documents;
            }
        }
        if (!event.getHeaders().containsKey(OPERATION) && timestampField != null) {
            Date timestamp;
            if (eventJson.containsKey(timestampField)) {
                try {
					String dateText = (String) eventJson.get(timestampField);
					timestamp = DATETIMEFORMATTER.parseDateTime(dateText).toDate();
					eventJson.remove(timestampField);
                } catch (Exception e) {
                    LOGGER.error("can't parse date ", e);
                    timestamp = new Date();
                }
            } else {
                timestamp = new Date();
            }
            eventJson.put(timestampField, timestamp);
        }

        documents.add(eventJson);

        return documents;
    }
    
	@Override
	public String toString() {
		return "MongoSink " + getName() + " { host: " + host + ", port: " + port + " }";
	}

    public enum CollectionModel {
        dynamic, single
    }
    
    private class MongoSinkThread implements Runnable {
    	
    	private static final double D_PERCENT = 100.0d;
    	private static final int I_PERCENT = 100;
		
    	private UUID eventMapsKey;
    	private String operation;
		private Map<UUID, Map<String, List<Document>>>	eventMaps;
		private int thread;
		private String sinkName;
		
		MongoSinkThread(String sinkName, UUID eventMapsKey, Map<UUID, Map<String, List<Document>>> eventMaps, String operation, int thread) {
			this.eventMapsKey = eventMapsKey;
			this.eventMaps = eventMaps;
			this.operation = operation;
			this.thread = thread;
			this.sinkName = sinkName;
		}

		public void run() {
			if (OP_UPSERT.equals(operation) || OP_INSERT.equals(operation)) {
				try {
					Map<String, List<Document>> eventMap = this.eventMaps.get(eventMapsKey);
					try {
						long start = 0L;
						long elapsed = 0L;
						
						if (monitoringThreads) {
							LOGGER.info(sinkName + ", Thread " + thread + " (" + this.eventMapsKey.toString() + ") started.");
						}
						
						if (LOGGER.isDebugEnabled() || monitoringThreads || this.eventMaps.size() >= maxThreads * (monitoringThreadsWarnTolerance / D_PERCENT)) {
							
							String percent = String.format("%.2f", (this.eventMaps.size() / (maxThreads * 1.0)) * I_PERCENT);
							
							String phrase = sinkName + ", " + operation + " is at " + percent + " percent of its capacity. " + this.eventMaps.size() + " threads.";
							
							LOGGER.debug(sinkName + ", " + operation + " accumulates: " + this.eventMaps.size() + " threads.");
							
							LOGGER.warn(phrase);
							
							start = System.currentTimeMillis();
						}
						
						if (OP_UPSERT.equals(operation)) {
							doUpsert(eventMap);
						} else if (OP_INSERT.equals(operation)) {
							saveEvents(eventMap);
						}
						
						if (monitoringThreads || elapsed > monitoringThreadsWarnSecOperation * MILLISECONDS) {
							elapsed = System.currentTimeMillis() - start;

							int[] params = new int[2];
							MongoSink.eventMapCount(eventMap, params, true);

							int events = params[0];
							int bytes = params[1];
							
							LOGGER.info(sinkName + ", " + operation + ", Thread " + thread + " (" + this.eventMapsKey.toString() + "), events: " + events + ", bytes: " + bytes + ", Total Time elapsed: " + MongoSink.parseMilliseconds(elapsed));
						}
						
						if (logFile != null) {
							for (Map.Entry<String, List<Document>> entry : eventMap.entrySet()) {
								for (Document dbObject : entry.getValue()) {
									try {
										logFile.append(dbObject.toString() + "\n");
										logFile.flush();
									} catch (IOException e) {
										LOGGER.error(ExceptionUtils.getMessage(e));
									}
								}
							}
						}
						
					} catch (Exception e) {
						LOGGER.error(ExceptionUtils.getMessage(e), e);
					} finally {
						int size = 0;
						for (Map.Entry<String, List<Document>> entry : eventMap.entrySet()) {
							size += entry.getValue().size();
						}
						sinkCounter.addToEventDrainSuccessCount(size);
						this.eventMaps.remove(eventMapsKey);
					}
				} catch (Exception e) {
					LOGGER.error(ExceptionUtils.getMessage(e), e);
				}

			}
		}

	}
    
	public static void printEventMaps(String sinkName, Map<UUID, Map<String, List<Document>>> eventMaps, String operation, boolean countBytes) throws UnsupportedEncodingException {

		int eventMapsTotalSize = 0;
		int eventMapsBytes = 0;
		int thread = 0;

		for (Map.Entry<UUID, Map<String, List<Document>>> entry : eventMaps.entrySet()) {

			Map<String, List<Document>> eventMap = entry.getValue();

			int[] params = new int[2];
			MongoSink.eventMapCount(eventMap, params, countBytes);

			int events = params[0];
			int bytes = params[1];

			eventMapsBytes += bytes;
			eventMapsTotalSize += events;

			thread++;
			LOGGER.info(sinkName + ", " + operation + ", Thread " + thread + " (" + entry.getKey().toString() + "), events: " + events + ", bytes: " + bytes);
		}

		if (!eventMaps.isEmpty()) {
			LOGGER.info(sinkName + ", " + operation + " threads: " + eventMaps.size() + " total events: " + eventMapsTotalSize + ", bytes: " + eventMapsBytes);
		}
	}
    
	public static void eventMapCount(Map<String, List<Document>> eventMap, int[] params, boolean countBytes) throws UnsupportedEncodingException {
		if (eventMap != null) {
			for (Map.Entry<String, List<Document>> entry : eventMap.entrySet()) {
				for (Document dbObject : entry.getValue()) {
					params[0]++;
					if (countBytes) {
						params[1] += dbObject.toString().getBytes(CHARSET).length;
					} else {
						params[1] = -1;
					}
				}
			}
		}
	}
    
	public static String parseMilliseconds(long elapsed) {
		long elapsedAux = elapsed;
		int ms = (int) (elapsedAux % MILLISECONDS);
		elapsedAux /= MILLISECONDS;
		int seconds = (int) (elapsedAux % SECONDS);
		elapsedAux /= SECONDS;
		int minutes = (int) (elapsedAux % MINUTES);
		elapsedAux /= MINUTES;
		int hours = (int) (elapsedAux % HOURS);
		return String.format("%02d:%02d:%02d:%04d", hours, minutes, seconds, ms);
	}
    
}
