package test.bcp.core.audit.flume.sink;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bcp.core.audit.flume.sink.MongoSink;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

/**
 * User: guoqiang.li
 * Date: 12-9-12
 * Time: 13:31
 */
public class MongoSinkTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(MongoSink.class);
	private static final Gson   GSON   = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").setPrettyPrinting().create();
	
	private static final int PORT = 27017;
	private static final String DBNAME = "test_events";
	private static final String COLLECTION = "test_log";
	private static final int BATCH_SIZE = 1;
	
	private static final int AGE = 11;
	private static final int EVENTS = 10;
	private static final int MOD_2 = 5;
	private static final int MOD_10 = 10;
	
	private static Mongo mongo;
	private static Context ctx = new Context();
	private static Channel channel;
	
	private static int blockProcess = -1;
	
	@BeforeClass
	public static void beforeTestClass() {
		mongo = new MongoClient("localhost", PORT);
		
		configure();
        
        final Context channelCtx = new Context();
        channelCtx.put("capacity", "1000000");
        channelCtx.put("transactionCapacity", "1000000");
        
        channel = new MemoryChannel();
        Configurables.configure(channel, channelCtx);
        
        blockProcess = EVENTS / BATCH_SIZE;

	}
	
	private static void configure() {
		final Map<String, String> ctxMap = new HashMap<String, String>();
		ctxMap.put(MongoSink.HOST, "localhost");
        ctxMap.put(MongoSink.PORT, "27017");
        ctxMap.put(MongoSink.DB_NAME, DBNAME);
        ctxMap.put(MongoSink.COLLECTION, COLLECTION);
        ctxMap.put(MongoSink.BATCH_SIZE, String.valueOf(BATCH_SIZE));
        
        ctx.clear();
        ctx.putAll(ctxMap);
	}
	
	@AfterClass
	public static void afterTestClass() {
		// mongo.dropDatabase(DBNAME);
		mongo.close();
	}
	
    @Test
    public void sinkDynamicSingleTest() throws EventDeliveryException, InterruptedException, UnsupportedEncodingException {
    	final String collectionName = "my_events";
    	
        ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.dynamic.name());
        
        final MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);
        
        configure();

        sink.setChannel(channel);
        sink.start();

        final Map<String, Object> msg = new HashMap<String, Object>();
        msg.put("age", AGE);
        msg.put("birthday", new Date().getTime());
        
        Transaction tx;

        for (int i = 0; i < EVENTS; i++) {
            tx = channel.getTransaction();
            tx.begin();
            msg.put("name", "test" + i);
            
            final Map<String, String> header = new HashMap<String, String>();
            header.put(MongoSink.COLLECTION, collectionName + i % MOD_10);
            
            final String jsonEvent = GSON.toJson(msg);
            LOGGER.info(jsonEvent);

            final Event e = EventBuilder.withBody(jsonEvent.getBytes(MongoSink.CHARSET), header);
            channel.put(e);
            tx.commit();
            tx.close();
        }
        
		for (int i = 0; i < blockProcess; i++) {
			sink.process();
		}
        
        sink.stop();
		
        final MongoDatabase db = ((MongoClient) mongo).getDatabase(DBNAME);
        
        for (int i = 0; i < EVENTS; i++) {
            msg.put("name", "test" + i);

			LOGGER.info("i = " + i);
			
			final MongoCollection<Document> collection = db.getCollection(collectionName + i % MOD_10);

			final FindIterable<Document> cursor = collection.find(new Document(msg));
			
			Assert.assertTrue(cursor.iterator().hasNext());
			
			final Document document = cursor.iterator().next();
			
            Assert.assertNotNull(document);
            Assert.assertEquals(document.get("name"), msg.get("name"));
            Assert.assertEquals(document.get("age"), msg.get("age"));
            Assert.assertEquals(document.get("birthday"), msg.get("birthday"));
        }
        
        for (int i = 0; i < EVENTS; i++) {
        	final MongoCollection<Document> collection = db.getCollection(collectionName + i % MOD_10);
			collection.drop();
        }
        
    }
    
    @Test
    public void sinkDynamicTest() throws EventDeliveryException, UnsupportedEncodingException {
    	final String dbName = "dynamic_test";
    	final String collectionName = "my_events";
    	
        ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.dynamic.name());
        final MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);
        
        configure();

        sink.setChannel(channel);
        sink.start();

        final Map<String, Object> msg = new HashMap<String, Object>();
        msg.put("age", AGE);
        msg.put("birthday", new Date().getTime());

        Transaction tx;

        for (int i = 0; i < EVENTS; i++) {
            tx = channel.getTransaction();
            tx.begin();
            msg.put("name", "test" + i);
            
            final Map<String, String> header = new HashMap<String, String>();
            header.put(MongoSink.DB_NAME, dbName + i % MOD_2);
            header.put(MongoSink.COLLECTION, collectionName + i % MOD_10);

            final Event e = EventBuilder.withBody(GSON.toJson(msg).getBytes(MongoSink.CHARSET), header);
            channel.put(e);
            tx.commit();
            tx.close();
        }
        
		for (int i = 0; i < blockProcess; i++) {
			sink.process();
		}
        
        sink.stop();

        for (int i = 0; i < EVENTS; i++) {
            msg.put("name", "test" + i);

            LOGGER.info("i = " + i);

            final MongoDatabase db = ((MongoClient) mongo).getDatabase(dbName + i % MOD_2);
            final MongoCollection<Document> collection = db.getCollection(collectionName + i % MOD_10);
            
            final FindIterable<Document> cursor = collection.find(new Document(msg));

            Assert.assertTrue(cursor.iterator().hasNext());
			
            final Document document = cursor.iterator().next();
            
            Assert.assertNotNull(document);
            Assert.assertEquals(document.get("name"), msg.get("name"));
            Assert.assertEquals(document.get("age"), msg.get("age"));
            Assert.assertEquals(document.get("birthday"), msg.get("birthday"));

        }
        
        for (int i = 0; i < EVENTS; i++) {
        	final MongoDatabase db = ((MongoClient) mongo).getDatabase(dbName + i % MOD_2);
        	final MongoCollection<Document> collection = db.getCollection(collectionName + i % MOD_10);
			collection.drop();
			db.drop();
        }
        
    }
    
    @Test
    public void sinkSingleModelTest() throws EventDeliveryException, UnsupportedEncodingException {
    	final String collectionName = "single_model_test";
    	
        ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.single.name());
        ctx.put(MongoSink.COLLECTION, collectionName);

        final MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);
        
        configure();

        sink.setChannel(channel);
        sink.start();

        final Transaction tx = channel.getTransaction();
        tx.begin();
        
        final Map<String, Object> msg = new HashMap<String, Object>();
        msg.put("name", "test");
        msg.put("age", AGE);
        msg.put("birthday", new Date().getTime());

        final Event e = EventBuilder.withBody(GSON.toJson(msg).getBytes(MongoSink.CHARSET));
        channel.put(e);
        tx.commit();
        tx.close();

        sink.process();
        sink.stop();

        final MongoDatabase db = ((MongoClient) mongo).getDatabase(DBNAME);
        
        final MongoCollection<Document> collection = db.getCollection(collectionName);
        final FindIterable<Document> cursor = collection.find(new Document(msg));
        
        Assert.assertTrue(cursor.iterator().hasNext());
        
        final Document document = cursor.iterator().next();
        Assert.assertNotNull(document);
        Assert.assertEquals(document.get("name"), msg.get("name"));
        Assert.assertEquals(document.get("age"), msg.get("age"));
        Assert.assertEquals(document.get("birthday"), msg.get("birthday"));
        
        collection.drop();
        
    }
    
    @SuppressWarnings("unchecked")
	@Test
    public void dbTest() {
    	final MongoDatabase db = ((MongoClient) mongo).getDatabase(DBNAME);
    	final MongoCollection<Document> collection = db.getCollection(COLLECTION);
    	
    	collection.insertOne(new Document(MapUtils.putAll(new HashMap<String, Object>(), new Object[]{"birthday", new Date().getTime(), "name", "lion", "age", AGE})));
    	
    	final MongoIterable<String> names = ((MongoClient) mongo).listDatabaseNames();

        Assert.assertNotNull(names);
        boolean hit = false;

        for (final String name : names) {
        	LOGGER.info(name);
            if (DBNAME.equals(name)) {
                hit = true;
                break;
            }
        }

        Assert.assertTrue(hit);
        
        collection.drop();
    }
    
	@SuppressWarnings("unchecked")
	@Test
    public void collectionTest() {
    	final MongoDatabase db = ((MongoClient) mongo).getDatabase(DBNAME);
    	final MongoCollection<Document> collection = db.getCollection(COLLECTION);
    	
    	collection.insertOne(new Document(MapUtils.putAll(new HashMap<String, Object>(), new Object[]{"birthday", new Date().getTime(), "name", "lion", "age", AGE})));

    	final MongoIterable<String> names = db.listCollectionNames();

        Assert.assertNotNull(names);
        boolean hit = false;

        for (final String name : names) {
            if (COLLECTION.equals(name)) {
                hit = true;
                break;
            }
        }

        Assert.assertTrue(hit);
        
        collection.drop();
    }
    
    @Test
    public void autoWrapTest() throws EventDeliveryException, UnsupportedEncodingException {
    	final String collectionName = "test_wrap";
    	
    	ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.single.name());
        ctx.put(MongoSink.AUTO_WRAP, Boolean.toString(true));
        ctx.put(MongoSink.COLLECTION, collectionName);

        final MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);
        
        configure();

        sink.setChannel(channel);
        sink.start();

        final Transaction tx = channel.getTransaction();
        tx.begin();
        
        final String msg = "2012/10/26 11:23:08 [error] 7289#0: *6430831 open() \"/usr/local/nginx/html/50x.html\" failed (2: No such file or directory), client: 10.160.105.161, server: sg15.redatoms.com, request: \"POST /mojo/ajax/embed HTTP/1.0\", upstream: \"fastcgi://unix:/tmp/php-fpm.sock:\", host: \"sg15.redatoms.com\", referrer: \"http://sg15.redatoms.com/mojo/mobile/package\"";

        final Event e = EventBuilder.withBody(msg.getBytes(MongoSink.CHARSET));
        channel.put(e);
        tx.commit();
        tx.close();

        sink.process();
        sink.stop();
        
        final MongoDatabase db = ((MongoClient) mongo).getDatabase(DBNAME);
        
        final MongoCollection<Document> collection = db.getCollection(collectionName);
        final FindIterable<Document> cursor = collection.find(new Document(MongoSink.DEFAULT_WRAP_FIELD, msg));
        
        Assert.assertTrue(cursor.iterator().hasNext());
        
        final Document document = cursor.iterator().next();
        Assert.assertNotNull(document);
        Assert.assertEquals(document.get(MongoSink.DEFAULT_WRAP_FIELD), msg);
        
        collection.drop();
    }
    
    @Test
    public void timestampNewFieldTest() throws EventDeliveryException, UnsupportedEncodingException {
    	final String dbName = "dynamic_test";
    	final String collectionName = "my_events";
    	final String tsField = "createdOn";
    	final MongoSink sink = new MongoSink();
    	
    	ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.dynamic.name());
        ctx.put(MongoSink.TIMESTAMP_FIELD, tsField);
        
        Configurables.configure(sink, ctx);

        configure();
        
        sink.setChannel(channel);
        sink.start();

        final Map<String, Object> msg = new HashMap<String, Object>();
        msg.put("age", AGE);
        msg.put("birthday", new Date().getTime());

        Transaction tx;

        for (int i = 0; i < EVENTS; i++) {
            tx = channel.getTransaction();
            tx.begin();
            msg.put("name", "test" + i);
            
            Map<String, String> header = new HashMap<String, String>();
            header.put(MongoSink.DB_NAME, dbName + i % MOD_2);
            header.put(MongoSink.COLLECTION, collectionName + i % MOD_10);

            Event e = EventBuilder.withBody(GSON.toJson(msg).getBytes(MongoSink.CHARSET), header);
            channel.put(e);
            tx.commit();
            tx.close();
        }
        
		for (int i = 0; i < blockProcess; i++) {
			sink.process();
		}
        
        sink.stop();
        
        for (int i = 0; i < EVENTS; i++) {
            msg.put("name", "test" + i);

            LOGGER.info("i = " + i);

            final MongoDatabase db = ((MongoClient) mongo).getDatabase(dbName + i % MOD_2);
            final MongoCollection<Document> collection = db.getCollection(collectionName + i % MOD_10);
            
            final FindIterable<Document> cursor = collection.find(new Document(msg));

            Assert.assertTrue(cursor.iterator().hasNext());
			
            final Document document = cursor.iterator().next();
            
            Assert.assertNotNull(document);
            Assert.assertEquals(document.get("name"), msg.get("name"));
            Assert.assertEquals(document.get("age"), msg.get("age"));
            Assert.assertEquals(document.get("birthday"), msg.get("birthday"));
            
            Assert.assertTrue(document.get(tsField) instanceof Date);

        }
        
        for (int i = 0; i < EVENTS; i++) {
        	final MongoDatabase db = ((MongoClient) mongo).getDatabase(dbName + i % MOD_2);
        	final MongoCollection<Document> collection = db.getCollection(collectionName + i % MOD_10);
			collection.drop();
			db.drop();
        }

    }
    
    @Test
    public void timestampExistingFieldTest() throws EventDeliveryException, UnsupportedEncodingException {
    	final String dbName = "dynamic_test";
    	final String collectionName = "my_events";
    	final String tsField = "createdOn";
    	final MongoSink sink = new MongoSink();
    	
        ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.dynamic.name());
        ctx.put(MongoSink.TIMESTAMP_FIELD, tsField);
        
        Configurables.configure(sink, ctx);
        
        configure();

        sink.setChannel(channel);
        sink.start();

        final Map<String, Object> msg = new HashMap<String, Object>();
        msg.put("age", AGE);
        msg.put("birthday", new Date().getTime());
        
        final String dateText = "2013-02-19T14:20:53+08:00";
        msg.put(tsField, dateText);

        Transaction tx;

        for (int i = 0; i < EVENTS; i++) {
            tx = channel.getTransaction();
            tx.begin();
            msg.put("name", "test" + i);
            
            final Map<String, String> header = new HashMap<String, String>();
            header.put(MongoSink.DB_NAME, dbName + i % MOD_2);
            header.put(MongoSink.COLLECTION, collectionName + i % MOD_10);

            final Event e = EventBuilder.withBody(GSON.toJson(msg).getBytes(MongoSink.CHARSET), header);
            channel.put(e);
            tx.commit();
            tx.close();
        }
        
		for (int i = 0; i < blockProcess; i++) {
			sink.process();
		}
        
        sink.stop();

        msg.put(tsField, MongoSink.DATETIMEFORMATTER.parseDateTime(dateText).toDate());
        
        for (int i = 0; i < EVENTS; i++) {
            msg.put("name", "test" + i);

            LOGGER.info("i = " + i);

            final MongoDatabase db = ((MongoClient) mongo).getDatabase(dbName + i % MOD_2);
            final MongoCollection<Document> collection = db.getCollection(collectionName + i % MOD_10);
            
            final FindIterable<Document> cursor = collection.find(new Document(msg));

            Assert.assertTrue(cursor.iterator().hasNext());
			
            final Document document = cursor.iterator().next();
            
            Assert.assertNotNull(document);
            Assert.assertEquals(document.get("name"), msg.get("name"));
            Assert.assertEquals(document.get("age"), msg.get("age"));
            Assert.assertEquals(document.get("birthday"), msg.get("birthday"));
            
            Assert.assertTrue(document.get(tsField) instanceof Date);
            
            LOGGER.info("ts = " + document.get(tsField));
        }
        
        for (int i = 0; i < EVENTS; i++) {
        	final MongoDatabase db = ((MongoClient) mongo).getDatabase(dbName + i % MOD_2);
        	final MongoCollection<Document> collection = db.getCollection(collectionName + i % MOD_10);
			collection.drop();
			db.drop();
        }

    }

	@Test
    public void sandbox() throws EventDeliveryException, UnsupportedEncodingException {
		final String dbName = "dynamic_test";
    	final String collectionName = "my_events";
		final String tsField = "createdOn";
		
		ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.dynamic.name());
        ctx.put(MongoSink.TIMESTAMP_FIELD, tsField);
        
        final MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);
        
        configure();
		
        final Map<String, Object> msg = new HashMap<String, Object>();
		
        final Map<String, Object> set = new HashMap<String, Object>();
        
        set.put("pid", "275");
        set.put("fac", "missin-do2");
        msg.put("$set", set);

        final Map<String, Object> inc = new HashMap<String, Object>();
        inc.put("sum", 1);

        msg.put("$inc", inc);
        msg.put("_id", "111111111111111111111111111");

        sink.setChannel(channel);
        sink.start();

        Transaction tx;

        tx = channel.getTransaction();
        tx.begin();

        final Map<String, String> header = new HashMap<String, String>();
        header.put(MongoSink.DB_NAME, dbName);
        header.put(MongoSink.COLLECTION, collectionName);
        header.put(MongoSink.OPERATION, MongoSink.OP_UPSERT);

        Event e = EventBuilder.withBody(GSON.toJson(msg).getBytes(MongoSink.CHARSET), header);
        channel.put(e);
        tx.commit();
        tx.close();
        sink.process();
        sink.stop();
        
        final MongoDatabase db = ((MongoClient) mongo).getDatabase(dbName);
        
        final MongoCollection<Document> collection = db.getCollection(collectionName);
        
        msg.clear();
        msg.put("pid", "275");
        msg.put("fac", "missin-do2");
        
        final FindIterable<Document> cursor = collection.find(new Document(msg));
        
        Assert.assertTrue(cursor.iterator().hasNext());
        
        final Document document = cursor.iterator().next();
        Assert.assertNotNull(document);
        
        Assert.assertEquals(document.get("pid"), msg.get("pid"));
        Assert.assertEquals(document.get("fac"), msg.get("fac"));
        Assert.assertEquals(document.get("sum"), 1);
        
        collection.drop();
        db.drop();
    }
	
    @Test
    public void upsertTest() throws EventDeliveryException, UnsupportedEncodingException {
    	final String dbName = "dynamic_test";
    	final String collectionName = "my_events";
    	final String id = "1111";
    	final String tsField = "createdOn";
    	
        ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.dynamic.name());
        ctx.put(MongoSink.TIMESTAMP_FIELD, tsField);
        
        final MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);
        
        configure();

        sink.setChannel(channel);
        sink.start();

        final Map<String, Object> msg = new HashMap<String, Object>();
        msg.put("age", AGE);
        msg.put("birthday", new Date().getTime());
        final String dateText = "2013-02-19T14:20:53+08:00";
        msg.put(tsField, dateText);

        Transaction tx;

        for (int i = 0; i < EVENTS; i++) {
            tx = channel.getTransaction();
            tx.begin();
            msg.put("_id", id + i);
            msg.put("name", "test" + i);
            
            final Map<String, String> header = new HashMap<String, String>();
            header.put(MongoSink.DB_NAME, dbName);
            header.put(MongoSink.COLLECTION, collectionName);

            final String jsonEvent = GSON.toJson(msg);
            LOGGER.info(jsonEvent);
            
            Event e = EventBuilder.withBody(jsonEvent.getBytes(MongoSink.CHARSET), header);
            channel.put(e);
            tx.commit();
            tx.close();
        }
        
		for (int i = 0; i < blockProcess; i++) {
			sink.process();
		}
        
        sink.stop();

        for (int i = 0; i < EVENTS; i++) {
            tx = channel.getTransaction();
            tx.begin();
            
            msg.put("_id", id + i);
            
            final Map<String, Object> set = new HashMap<String, Object>();
            
            set.put("name", "test" + i * MOD_10);
            msg.put("$set", set);
            
            final Map<String, String> header = new HashMap<String, String>();
            header.put(MongoSink.DB_NAME, dbName);
            header.put(MongoSink.COLLECTION, collectionName);
            header.put(MongoSink.OPERATION, MongoSink.OP_UPSERT);

            Event e = EventBuilder.withBody(GSON.toJson(msg).getBytes(MongoSink.CHARSET), header);
            channel.put(e);
            tx.commit();
            tx.close();
        }
        
		for (int i = 0; i < blockProcess; i++) {
			sink.process();
		}
        
        sink.stop();

        msg.put(tsField, MongoSink.DATETIMEFORMATTER.parseDateTime(dateText).toDate());
        
        for (int i = 0; i < EVENTS; i++) {
            LOGGER.info("i = " + i);

            final MongoDatabase db = ((MongoClient) mongo).getDatabase(dbName);
            final MongoCollection<Document> collection = db.getCollection(collectionName);
            
            final FindIterable<Document> cursor = collection.find(new Document("_id", id + i));

            Assert.assertTrue(cursor.iterator().hasNext());
			
            final Document document = cursor.iterator().next();
            
            Assert.assertNotNull(document);
            Assert.assertEquals(document.get("name"), "test" + i * MOD_10);
            Assert.assertEquals(document.get("age"), msg.get("age"));
            Assert.assertEquals(document.get("birthday"), msg.get("birthday"));
            
            Assert.assertTrue(document.get(tsField) instanceof Date);
            
            LOGGER.info("ts = " + document.get(tsField));
            LOGGER.info("_id = " + document.get("_id"));
        }
        
        for (int i = 0; i < EVENTS; i++) {
        	final MongoDatabase db = ((MongoClient) mongo).getDatabase(dbName);
        	final MongoCollection<Document> collection = db.getCollection(collectionName);
			collection.drop();
			db.drop();
        }

    }

    @Test
    public void testUpsertCep() throws EventDeliveryException {
    	final String dbName = "dynamic_dbjj";
    	final String collectionName = "my_eventsjj";
        
    	final Map<String, Object> row = new HashMap<String, Object>();
    	final Map<String, Object> inc = new HashMap<String, Object>();
    	final Map<String, Object> currentDate = new HashMap<String, Object>();
        final Map<String, Object> set = new HashMap<String, Object>();

        set.put("eventClass", "com.bancsabadell.cep.server.event.bean.internal.notification.proteo.ProteoPublishNotificationOutEvent");
        set.put("eventType", "com.bancsabadell.cep.server.event.bean.OutEventI");		
        set.put("startDate", "2014-06-16T09:00:00.000Z");
        set.put("stopDate", "2014-06-16T10:00:00.000Z");
        
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
		final String dateString = simpleDateFormat.format(new Date());
		
        set.put("modified", dateString);
        row.put("$set", set);
  
        inc.put("eventCount", 1);

        row.put("$inc", inc);
        
        currentDate.put("timestamp", true);
        
        row.put("$currentDate", currentDate);
        
        row.put("eventClass", "com.bancsabadell.cep.server.event.bean.internal.notification.proteo.ProteoPublishNotificationOutEvent");
        row.put("eventType", "com.bancsabadell.cep.server.event.bean.OutEventI");		
        row.put("startDate", "2014-06-16T09:00:00.000Z");
        row.put("stopDate", "2014-06-16T10:00:00.000Z");
        
        //ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.dynamic.name());
        //String tsField = "createdOn";
        //ctx.put(MongoSink.TIMESTAMP_FIELD, tsField);
		ctx.put(MongoSink.PK, "eventClass eventType");
		ctx.put(MongoSink.DATE_FIELDS, "startDate stopDate modified");
		ctx.put(MongoSink.DB_NAME, dbName);
		ctx.put(MongoSink.COLLECTION, collectionName);
		ctx.put(MongoSink.OPERATION, "upsert");
		// ctx.put(MongoSink.ACKNOWLEDGED_ENABLED, "false");
		
        MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);

        configure();
        
        sink.setChannel(channel);
        sink.start();

        Transaction tx;

        tx = channel.getTransaction();
        tx.begin();

        final Map<String, String> header = new HashMap<String, String>();
        //header.put(MongoSink.COLLECTION, "my_eventsjj");
        //header.put(MongoSink.DB_NAME, "dynamic_dbjj");
        //header.put(MongoSink.OPERATION, MongoSink.OP_UPSERT);
        
        final String event = GSON.toJson(row);
        
        Event e = EventBuilder.withBody(event.getBytes(), header);
        channel.put(e);
        tx.commit();
        tx.close();
        sink.process();
        sink.stop();
        
        sink.start();

        tx = channel.getTransaction();
        tx.begin();
        
        e = EventBuilder.withBody(event.getBytes(), header);
        channel.put(e);
        tx.commit();
        tx.close();
        sink.process();
        sink.stop();
        
        final MongoDatabase db = ((MongoClient) mongo).getDatabase(dbName);
        final MongoCollection<Document> collection = db.getCollection(collectionName);
        
        final Document query = new Document()
    		.append("eventClass", "com.bancsabadell.cep.server.event.bean.internal.notification.proteo.ProteoPublishNotificationOutEvent")
	        .append("eventType", "com.bancsabadell.cep.server.event.bean.OutEventI");
        
        final FindIterable<Document> cursor = collection.find(query);

        Assert.assertTrue(cursor.iterator().hasNext());
		
        final Document document = cursor.iterator().next();
        
        Assert.assertNotNull(document);
        Assert.assertEquals(document.get("eventClass"), row.get("eventClass"));
        Assert.assertEquals(document.get("eventType"), row.get("eventType"));
		Assert.assertEquals(((Date) document.get("startDate")).getTime(), MongoSink.DATETIMEFORMATTER.parseDateTime((String) row.get("startDate")).getMillis());
		Assert.assertEquals(((Date) document.get("stopDate")).getTime(), MongoSink.DATETIMEFORMATTER.parseDateTime((String) row.get("stopDate")).getMillis());
        Assert.assertEquals(document.get("eventCount"), 2);
        
        collection.drop();
        db.drop();
    }
	
	@Test
	public void testUpsert() throws EventDeliveryException {
		final String dbName = "archetype_database";
		final String collectionName = "event_message";
		
		final Map<String, Object> row = new HashMap<String, Object>();
		final Map<String, Object> set = new HashMap<String, Object>();
        
		final String uUID = java.util.UUID.randomUUID().toString();
//        set.put("thread", 1);
//        set.put("UUID", uUID);
//        set.put("host", "host1");
//        set.put("node", "node1");
//        set.put("date", "2014-07-06T16:32:36.314+0200");
        //set.put("date", "2014-06-16T10:00:00.000Z");

        row.put("$set", set);
        
        final Map<String, Object> currentDate = new HashMap<String, Object>();
        currentDate.put("timestamp", true);
        
        row.put("$currentDate", currentDate);
        
        
        row.put("UUID", "1");
        row.put("thread", 1);
        row.put("host", "host1");
        row.put("node", "node1");
        row.put("date", "2014-07-06T16:32:36.314+0200");
        
        //ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.dynamic.name());
        //String tsField = "createdOn";
        //ctx.put(MongoSink.TIMESTAMP_FIELD, tsField);
        
        //ctx.put(MongoSink.HOST, "172.22.65.140");
        //ctx.put(MongoSink.AUTHENTICATION_ENABLED, "true");
        //ctx.put(MongoSink.USERNAME, "hadoop");
        //ctx.put(MongoSink.PASSWORD, "hadoop14**");
        
//		ctx.put(MongoSink.PK, "UUID");
		ctx.put(MongoSink.DATE_FIELDS, "date modified");
		//ctx.put(MongoSink.AUTHENTICATION_DB_NAME, "bsabadell");
		//ctx.put(MongoSink.DB_NAME, "cep");
		ctx.put(MongoSink.DB_NAME, dbName);
		ctx.put(MongoSink.COLLECTION, collectionName);
		ctx.put(MongoSink.OPERATION, "upsert");
		ctx.put(MongoSink.THREADED, "false");
		ctx.put(MongoSink.MONITORING_THREADS, "true");
		ctx.put(MongoSink.ACKNOWLEDGED_ENABLED, "true");
		ctx.put(MongoSink.SOCKET_KEEP_ALIVE, "true");
		
        final MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);
        
        configure();

        sink.setChannel(channel);
        sink.start();

        Transaction tx;

        tx = channel.getTransaction();
        tx.begin();

        final Map<String, String> header = new HashMap<String, String>();
        // header.put(MongoSink.COLLECTION, "event_message");
        // header.put(MongoSink.DB_NAME, "archetype_database");
        // header.put(MongoSink.OPERATION, MongoSink.OP_UPSERT);
        
        final String event = GSON.toJson(row);
        
        LOGGER.info(event);
        
        final Event e = EventBuilder.withBody(event.getBytes(), header);
        channel.put(e);
        tx.commit();
        tx.close();
        sink.process();
        sink.stop();
        
//        final MongoDatabase db = ((MongoClient) mongo).getDatabase(dbName);
//        final MongoCollection<Document> collection = db.getCollection(collectionName);
//        
//        final Document query = new Document()
//    		.append("UUID", uUID);
//        
//        final FindIterable<Document> cursor = collection.find(query);
//
//        Assert.assertTrue(cursor.iterator().hasNext());
//		
//        final Document document = cursor.iterator().next();
//        
//        Assert.assertNotNull(document);
//        Assert.assertEquals(document.get("host"), row.get("host"));
//        Assert.assertEquals(document.get("node"), row.get("node"));
//        Assert.assertEquals(document.get("thread"), row.get("thread"));
//        
//        Assert.assertTrue(document.get("date") instanceof Date);
//        Assert.assertTrue(document.get("modified") instanceof Date);
//        Assert.assertTrue(document.get("timestamp") instanceof Date);
//        
//        collection.drop();
//        db.drop();
	}
    
}
