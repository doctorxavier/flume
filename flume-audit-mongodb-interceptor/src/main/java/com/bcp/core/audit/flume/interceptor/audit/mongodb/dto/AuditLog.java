package com.bcp.core.audit.flume.interceptor.audit.mongodb.dto;

import java.io.Serializable;

import org.joda.time.DateTime;

import com.google.gson.annotations.SerializedName;

public class AuditLog implements Serializable {

	private static final long	serialVersionUID	= 1L;

	@SerializedName("_id")
	private String				id;

	private String				cic;

	private String				idc;

	private String				eventType;

	private String				eventName;

	private DateTime			dateTime;

	private Integer				date;

	private Short				time;

	private Short				sec;

	private Short				millisec;

	private String				transactionId;

	private String				operationCod;

	private String				operationTypeCod;

	private String				errorCod;

	private String				clientId;

	private String				sessionId;

	private String				sessionToken;

	private String				clientIp;

	private String				clientData;

	private String				serverNode;

	private Long				elapsedTime;

	private String				dataType;

	private String				dataFormat;

	private Object				data;

	private String				dataToken;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getCic() {
		return cic;
	}

	public void setCic(String cic) {
		this.cic = cic;
	}

	public String getIdc() {
		return idc;
	}

	public void setIdc(String idc) {
		this.idc = idc;
	}

	public String getEventType() {
		return eventType;
	}

	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	public String getEventName() {
		return eventName;
	}

	public void setEventName(String eventName) {
		this.eventName = eventName;
	}

	public DateTime getDateTime() {
		return dateTime;
	}

	public void setDateTime(DateTime dateTime) {
		this.dateTime = dateTime;
	}

	public Integer getDate() {
		return date;
	}

	public void setDate(Integer date) {
		this.date = date;
	}

	public Short getTime() {
		return time;
	}

	public void setTime(Short time) {
		this.time = time;
	}

	public Short getSec() {
		return sec;
	}

	public void setSec(Short sec) {
		this.sec = sec;
	}

	public Short getMillisec() {
		return millisec;
	}

	public void setMillisec(Short millisec) {
		this.millisec = millisec;
	}

	public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

	public String getOperationCod() {
		return operationCod;
	}

	public void setOperationCod(String operationCod) {
		this.operationCod = operationCod;
	}

	public String getOperationTypeCod() {
		return operationTypeCod;
	}

	public void setOperationTypeCod(String operationTypeCod) {
		this.operationTypeCod = operationTypeCod;
	}

	public String getErrorCod() {
		return errorCod;
	}

	public void setErrorCod(String errorCod) {
		this.errorCod = errorCod;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getSessionToken() {
		return sessionToken;
	}

	public void setSessionToken(String sessionToken) {
		this.sessionToken = sessionToken;
	}

	public String getClientIp() {
		return clientIp;
	}

	public void setClientIp(String clientIp) {
		this.clientIp = clientIp;
	}

	public String getClientData() {
		return clientData;
	}

	public void setClientData(String clientData) {
		this.clientData = clientData;
	}

	public String getServerNode() {
		return serverNode;
	}

	public void setServerNode(String serverNode) {
		this.serverNode = serverNode;
	}

	public Long getElapsedTime() {
		return elapsedTime;
	}

	public void setElapsedTime(Long elapsedTime) {
		this.elapsedTime = elapsedTime;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public String getDataFormat() {
		return dataFormat;
	}

	public void setDataFormat(String dataFormat) {
		this.dataFormat = dataFormat;
	}

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}

	public String getDataToken() {
		return dataToken;
	}

	public void setDataToken(String dataToken) {
		this.dataToken = dataToken;
	}

}
