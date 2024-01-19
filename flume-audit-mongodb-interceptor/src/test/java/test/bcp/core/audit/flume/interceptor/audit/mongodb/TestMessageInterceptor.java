package test.bcp.core.audit.flume.interceptor.audit.mongodb;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.SerializationUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.InterceptorBuilderFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bcp.core.audit.flume.interceptor.audit.mongodb.MessageInterceptor;
import com.bcp.core.audit.flume.interceptor.audit.mongodb.dto.AuditInfo;
import com.bcp.core.audit.flume.interceptor.audit.mongodb.dto.AuditLog;
import com.bcp.core.audit.flume.sink.MongoSink;
import com.google.common.base.Charsets;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

public class TestMessageInterceptor {

	private static final Logger	LOGGER				= LoggerFactory.getLogger(TestMessageInterceptor.class);

	private static final String	MESSAGE				= "ALEX|ZELADA|28|27/02/1987";
	private static final Long	ELAPSEDTIME			= (long) 10999;
	private static final Short	MILLISEC			= 344;

	private MessageInterceptor	messageInterceptor	= new MessageInterceptor();

	// test
	@Test
	@Ignore
	public void intercept1() throws EventDeliveryException {

		LOGGER.info("Start intercept");

		Event event = EventBuilder.withBody(MESSAGE, Charsets.UTF_8);

		event = messageInterceptor.intercept(event);

		Map<String, String> headers = event.getHeaders();

		for (String header : headers.keySet()) {
			LOGGER.info(header + ": " + headers.get(header));
		}

		byte[] body = event.getBody();

		String eventString = new String(body, 0, body.length);

		LOGGER.info(eventString);

		LOGGER.info("End intercept");
	}

	@Test
	public void interceptMongoDB() throws Exception {
		final Mongo mongo = new MongoClient("localhost", 27017);

		final Context ctx = new Context(); 
        
		DateTime dateTime = new DateTime(DateTimeZone.UTC);
		//org.joda.time.format.DateTimeFormatter f = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
		//DateTime dateTime = before.toDateTime("2012-01-10 23:13:26");
		Object data = populateAuditInfo("ALEXANDER");
		Integer date = 2;
		Short time = 4;
		Short sec = 3;

		AuditLog objAuditLog = populateAuditLog("id", "cic", "idc", "eventType",dateTime, date, time, sec, MILLISEC, "eventName", "transactionId", "operationCod",
				"operationTypeCod", "errorCod", "clientId", "sessionId", "sessionToken", "clientIp", "clientData", "serverNode", ELAPSEDTIME, "dataType",
				"dataFormat", data, "dataToken");

		byte[] auditInfoBytes = SerializationUtils.serialize(objAuditLog);

		Map<String, String> ctxMap = new HashMap<String, String>();
		ctxMap.put(MongoSink.HOST, "localhost");
		ctxMap.put(MongoSink.PORT, "27017");
		ctxMap.put(MongoSink.DB_NAME, "test_events");
		ctxMap.put(MongoSink.COLLECTION, "test_log");
		ctxMap.put(MongoSink.BATCH_SIZE, "1");

		ctx.putAll(ctxMap);

		final Channel channel;

		Context channelCtx = new Context();
		channelCtx.put("capacity", "1000000");
		channelCtx.put("transactionCapacity", "1000000");
		channel = new MemoryChannel();
		Configurables.configure(channel, channelCtx);

		Interceptor.Builder builder = InterceptorBuilderFactory.newInstance("com.bcp.core.audit.flume.interceptor.audit.mongodb.MessageInterceptor$Builder");
		Interceptor interceptor = builder.build();
		Event eventBeforeIntercept = EventBuilder.withBody(auditInfoBytes);
		Event eventAfterIntercept = interceptor.intercept(eventBeforeIntercept);

		String json = new String(eventAfterIntercept.getBody());
		LOGGER.info(json);
		ctx.put(MongoSink.HOST, "localhost");
		// ctx.put(MongoSink.PK, "UUID");
		ctx.put(MongoSink.DATE_FIELDS, "date modified");
		ctx.put(MongoSink.AUTHENTICATION_DB_NAME, "bsabadell");
		ctx.put(MongoSink.DB_NAME, "cep");
		ctx.put(MongoSink.DB_NAME, "mi");
		ctx.put(MongoSink.COLLECTION, "final");
		// ctx.put(MongoSink.OPERATION, "upsert");
		ctx.put(MongoSink.THREADED, "false");
		ctx.put(MongoSink.MONITORING_THREADS, "false");
		ctx.put(MongoSink.ACKNOWLEDGED_ENABLED, "true");
		ctx.put(MongoSink.SOCKET_KEEP_ALIVE, "true");

		MongoSink sink = new MongoSink();

		Configurables.configure(sink, ctx);

		sink.setChannel(channel);
		sink.start();

		Transaction tx;

		tx = channel.getTransaction();
		tx.begin();

		final Map<String, String> header = new HashMap<String, String>();

		Event e = EventBuilder.withBody(json.getBytes(), header);
		channel.put(e);
		tx.commit();
		tx.close();
		sink.process();
		sink.stop();

		mongo.close();
	}

	public AuditInfo populateAuditInfo(String nombre) {

		AuditInfo auditInfo = new AuditInfo();

		auditInfo.setNombre(nombre);

		return auditInfo;
	}

	public static AuditLog populateAuditLog(String id, String cic, String idc, String eventType, DateTime dateTime, Integer date, Short time, Short sec,
			Short millisec, String eventName, String transactionId, String operationCod, String operationTypeCod, String errorCod, String clientId,
			String sessionId, String sessionToken, String clientIp, String clientData, String serverNode, Long elapsedTime, String dataType, String dataFormat,
			Object data, String dataToken) {

		AuditLog auditLog = new AuditLog();

		auditLog.setId(clientId);
		auditLog.setCic(cic);
		auditLog.setIdc(idc);
		auditLog.setEventType(eventType);
		auditLog.setDateTime(dateTime);
		auditLog.setDate(date);
		auditLog.setTime(time);
		auditLog.setSec(sec);
		auditLog.setMillisec(millisec);
		auditLog.setEventName(eventName);
		auditLog.setTransactionId(transactionId);
		auditLog.setOperationCod(operationCod);
		auditLog.setOperationTypeCod(operationTypeCod);
		auditLog.setErrorCod(errorCod);
		auditLog.setClientId(clientId);
		auditLog.setSessionId(sessionId);
		auditLog.setSessionToken(sessionToken);
		auditLog.setClientIp(clientIp);
		auditLog.setClientData(clientData);
		auditLog.setServerNode(serverNode);
		auditLog.setElapsedTime(elapsedTime);
		auditLog.setDataType(dataType);
		auditLog.setDataFormat(dataFormat);
		auditLog.setData(data);
		auditLog.setDataToken(dataToken);

		return auditLog;

	}

}
