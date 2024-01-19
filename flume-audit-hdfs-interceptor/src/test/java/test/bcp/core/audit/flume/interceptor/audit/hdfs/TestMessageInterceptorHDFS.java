package test.bcp.core.audit.flume.interceptor.audit.hdfs;

import java.util.Map;

import org.apache.commons.lang.SerializationUtils;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.InterceptorBuilderFactory;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bcp.core.audit.flume.interceptor.audit.hdfs.MessageInterceptorHDFS;
import com.bcp.core.audit.flume.interceptor.audit.hdfs.dto.AuditInfo;
import com.bcp.core.audit.flume.interceptor.audit.hdfs.dto.AuditLog2;
import com.google.common.base.Charsets;

public class TestMessageInterceptorHDFS {

	private static final Logger		LOGGER				= LoggerFactory.getLogger(TestMessageInterceptorHDFS.class);

	private static final String		MESSAGE				= "ALEX|ZELADA|28|27/02/1987";
	private static final Long		ELAPSEDTIME			= (long) 10999;
	private static final Short		MILLISEC			= 344;
	
	private MessageInterceptorHDFS	messageInterceptor	= new MessageInterceptorHDFS();

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
	public void interceptHDFS() throws Exception {

		
		org.joda.time.format.DateTimeFormatter f = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
		DateTime dateTime = f.parseDateTime("2012-01-10 23:13:26");
		Object data = populateAuditInfo("PROBANDO VALORES OPCIONALES Y ADICIONALES");
		Integer date = 2;
		Short time = 4;
		Short sec = 3;


		AuditLog2 objAuditLog2 = populateAuditLog("id", "cic", "idc","messageId", "eventType", dateTime, date, time, sec, MILLISEC, "eventName", "transactionId",
				"operationCod", "operationTypeCod", "errorCod", "clientId", "sessionId", "sessionToken", "clientIp", "clientData", "serverNode", ELAPSEDTIME,
				"dataType", "dataFormat", data,"dataToken","dataToken2");

		byte[] auditInfoBytes = SerializationUtils.serialize(objAuditLog2);

		Interceptor.Builder builder = InterceptorBuilderFactory.newInstance("com.bcp.core.audit.flume.interceptor.audit.hdfs.MessageInterceptorHDFS$Builder");
		Interceptor interceptor = builder.build();
		Event eventBeforeIntercept = EventBuilder.withBody(auditInfoBytes);
		Event eventAfterIntercept = interceptor.intercept(eventBeforeIntercept);
		String csv = new String(eventAfterIntercept.getBody());
			
		LOGGER.info(csv);
	}

	public AuditInfo populateAuditInfo(String nombre) {

		AuditInfo auditInfo = new AuditInfo();

		auditInfo.setNombre(nombre);

		return auditInfo;
	}

	public static AuditLog2 populateAuditLog(String id, String cic, String idc, String messageId,String eventType, DateTime dateTime, Integer date, Short time, Short sec,
			Short millisec, String eventName, String transactionId, String operationCod, String operationTypeCod, String errorCod, String clientId,
			String sessionId, String sessionToken, String clientIp, String clientData, String serverNode, Long elapsedTime, String dataType, String dataFormat,
			Object data, String dataToken,String dataToken2) {

		AuditLog2 auditLog = new AuditLog2();

		auditLog.setId(id);
		auditLog.setCic(cic);
		auditLog.setIdc(idc);
		auditLog.setMessageId(messageId);
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
		auditLog.setDataToken2(dataToken);
		
		return auditLog;

	}
}
