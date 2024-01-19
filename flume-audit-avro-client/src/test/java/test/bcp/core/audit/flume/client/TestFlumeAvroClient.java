package test.bcp.core.audit.flume.client;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bcp.core.audit.flume.client.FlumeAvroClient;
import com.bcp.core.audit.flume.client.dto.AuditLog;
import com.bcp.core.audit.flume.client.exception.AuditLogException;
import com.bcp.core.audit.flume.util.PojoUtils;
import com.bcp.core.audit.flume.util.gson.DateTimeConverter;
import com.bcp.core.audit.flume.util.gson.LocalDateConverter;
import com.bcp.core.audit.flume.util.parser.Parser;
import com.bcp.core.audit.flume.util.parser.json.JsonParser;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import test.bcp.core.audit.flume.client.dto.ComplexType;

public class TestFlumeAvroClient {
	
	private static final Logger	LOGGER	= LoggerFactory.getLogger(TestFlumeAvroClient.class);
	
	private static final Gson   GSON   = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    		.registerTypeAdapter(DateTime.class, new DateTimeConverter())
    		.registerTypeAdapter(LocalDate.class, new LocalDateConverter()).setPrettyPrinting().create();
	
	private static final Parser<ComplexType>	JSON_PARSER	= new JsonParser<ComplexType>(ComplexType.class, false);
	
	@Test(expected = AuditLogException.class)
	public void testSendDataToFlume() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, UnsupportedEncodingException, AuditLogException {
		final FlumeAvroClient flumeAvroClient = new FlumeAvroClient();
		
		final test.bcp.core.audit.flume.client.dto.AuditLog auxInAuditLog = new test.bcp.core.audit.flume.client.dto.AuditLog();
		this.initializeInAuditLog(auxInAuditLog);
		
		try {
			flumeAvroClient.sendDataToFlume(SerializationUtils.serialize(auxInAuditLog));
		} catch (Exception e) {
			LOGGER.error(ExceptionUtils.getMessage(e), e);
			throw e;
		}
		
	}

	@Test
	@Ignore
	public void test() throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		final test.bcp.core.audit.flume.client.dto.AuditLog auxInAuditLog = new test.bcp.core.audit.flume.client.dto.AuditLog();
		this.initializeInAuditLog(auxInAuditLog);
		LOGGER.info(auxInAuditLog.getData());
		
		final byte[] inAuditLogBytes = SerializationUtils.serialize(auxInAuditLog);
		final Object inAuditLogObject = SerializationUtils.deserialize(inAuditLogBytes);
		
		if (inAuditLogObject.getClass().isAssignableFrom(test.bcp.core.audit.flume.client.dto.AuditLog.class)) {
			final test.bcp.core.audit.flume.client.dto.AuditLog inAuditLog = (test.bcp.core.audit.flume.client.dto.AuditLog) inAuditLogObject;
			final Object data = GSON.fromJson(inAuditLog.getData(), Object.class);
			
			LOGGER.info(GSON.toJson(inAuditLog));
			
			final AuditLog auditLog = new AuditLog();
			this.initializeAuditLog(auditLog);
			
			auditLog.setData(data);
			LOGGER.info(GSON.toJson(auditLog));
		}
		
		
	}
	
	private void initializeAuditLog(final AuditLog auditLog) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		PojoUtils.initializePojo(auditLog);
		final ComplexType complexType = getComplexType();
		auditLog.setData(complexType);
	}
	
	private void initializeInAuditLog(final test.bcp.core.audit.flume.client.dto.AuditLog inAuditLog) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		PojoUtils.initializePojo(inAuditLog);
		final ComplexType complexType = getComplexType();
		inAuditLog.setData(JSON_PARSER.toString(complexType));
	}
	
	private ComplexType getComplexType() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		final ComplexType complexType = new ComplexType();
		
		PojoUtils.initializePojo(complexType);
		
		final List<Object> objectsList = new ArrayList<Object>();
		for (int i = 0; i < 5; i++) {
			final test.bcp.core.audit.flume.client.dto.AuditLog innerAuditLog = new test.bcp.core.audit.flume.client.dto.AuditLog();
			PojoUtils.initializePojo(innerAuditLog);
			objectsList.add(innerAuditLog);
		}
		complexType.setObjectsArray(objectsList.toArray());
		complexType.setObjectsList(objectsList);
		
		final Map<String, Object> objectsMap = new HashMap<String, Object>();
		for (int i = 0; i < 5; i++) {
			final test.bcp.core.audit.flume.client.dto.AuditLog innerAuditLog = new test.bcp.core.audit.flume.client.dto.AuditLog();
			PojoUtils.initializePojo(innerAuditLog);
			objectsMap.put(String.valueOf(i), innerAuditLog);
		}
		complexType.setObjectsMap(objectsMap);
		
		final Set<Object> objectsSet = new HashSet<Object>();
		for (int i = 0; i < 5; i++) {
			final test.bcp.core.audit.flume.client.dto.AuditLog innerAuditLog = new test.bcp.core.audit.flume.client.dto.AuditLog();
			PojoUtils.initializePojo(innerAuditLog);
			objectsSet.add(innerAuditLog);
		}
		complexType.setObjectsSet(objectsSet);
		
		return complexType;
	}

}
