package com.bcp.core.audit.flume.client;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bcp.core.audit.flume.client.dto.AuditLog;
import com.bcp.core.audit.flume.client.exception.AuditLogException;
import com.bcp.core.audit.flume.util.gson.DateTimeConverter;
import com.bcp.core.audit.flume.util.gson.LocalDateConverter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class FlumeAvroClient {

	private static final Logger	LOGGER	= LoggerFactory.getLogger(FlumeAvroClient.class);
	
	private static final Gson   GSON   = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    		.registerTypeAdapter(DateTime.class, new DateTimeConverter())
    		.registerTypeAdapter(LocalDate.class, new LocalDateConverter()).setPrettyPrinting().create();

	private RpcClient			client;
	private String				hostname;
	private int					port;

	public void init(String hostname, int port) {
		// Setup the RPC connection
		this.hostname = hostname;
		this.port = port;
		this.client = RpcClientFactory.getDefaultInstance(hostname, port);
		// Use the following method to create a thrift client (instead of the
		// above line):
		// this.client = RpcClientFactory.getThriftInstance(hostname, port);
	}

	public void sendDataToFlume(final byte[] inAuditLogBytes) throws AuditLogException, UnsupportedEncodingException {
		
		final Object inAuditLogObject = SerializationUtils.deserialize(inAuditLogBytes);
		final AuditLog auditLog = new AuditLog();
		
		try {
			BeanUtils.copyProperties(auditLog, inAuditLogObject);

			if (auditLog.getData() instanceof String) {
				final String dataString = (String) auditLog.getData();
				final Object data = GSON.fromJson(dataString, Object.class);
				auditLog.setData(data);
			}
			
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace(GSON.toJson(auditLog));
			}
			
		} catch (IllegalAccessException | InvocationTargetException | SecurityException e) {
			throw new AuditLogException(AuditLogException.U0001);
		}

		append(SerializationUtils.serialize(auditLog));
	}
	
	private void append(final byte[] msg) throws AuditLogException {
		Event event = EventBuilder.withBody(msg);

		if (client == null) {
			throw new AuditLogException(AuditLogException.U0002);
		}
		
		try {
			client.append(event);
		} catch (EventDeliveryException e) {
			// clean up and recreate the client
			client.close();
			client = null;
			client = RpcClientFactory.getDefaultInstance(hostname, port);
			// Use the following method to create a thrift client (instead of
			// the above line):
			// this.client = RpcClientFactory.getThriftInstance(hostname, port);
			
			throw new AuditLogException(e);
		}
	}

	public void cleanUp() {
		client.close();
	}

}
