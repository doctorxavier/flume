package com.bcp.core.audit.flume.interceptor.behavior.hdfs.dto;

import java.io.Serializable;

import org.joda.time.DateTime;

import com.google.gson.annotations.SerializedName;

public class BehaivorLog implements Serializable {

	private static final long	serialVersionUID	= 1L;

	@SerializedName("_id")
	private String				id;
	
	private DateTime			hour;
	
	private Short				second;
	
	private Short				millisecond;
	
	private DateTime			dateTime;
	
	private DateTime			timestamp;
	
	private String				node;
	
	private String				clientIp;
	
	private String				clientInfo;
	
	private String				referer;
	
	private String				cic;
	
	private String				idc;
	
	private String				sessionId;
	
	private String				sessionToken;
	
	private String				requestUrl;
	
	private String				view;
	
	private String				flow;
	
	private String				component;
	
	private DateTime			date;
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public DateTime getDate() {
		return date;
	}

	public void setDate(DateTime date) {
		this.date = date;
	}

	public DateTime getHour() {
		return hour;
	}

	public void setHour(DateTime hour) {
		this.hour = hour;
	}

	public Short getSecond() {
		return second;
	}

	public void setSecond(Short second) {
		this.second = second;
	}

	public Short getMillisecond() {
		return millisecond;
	}

	public void setMillisecond(Short millisecond) {
		this.millisecond = millisecond;
	}

	public DateTime getDateTime() {
		return dateTime;
	}

	public void setDateTime(DateTime dateTime) {
		this.dateTime = dateTime;
	}

	public DateTime getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(DateTime timestamp) {
		this.timestamp = timestamp;
	}

	public String getNode() {
		return node;
	}

	public void setNode(String node) {
		this.node = node;
	}

	public String getClientIp() {
		return clientIp;
	}

	public void setClientIp(String clientIp) {
		this.clientIp = clientIp;
	}

	public String getClientInfo() {
		return clientInfo;
	}

	public void setClientInfo(String clientInfo) {
		this.clientInfo = clientInfo;
	}

	public String getReferer() {
		return referer;
	}

	public void setReferer(String referer) {
		this.referer = referer;
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

	public String getRequestUrl() {
		return requestUrl;
	}

	public void setRequestUrl(String requestUrl) {
		this.requestUrl = requestUrl;
	}

	public String getView() {
		return view;
	}

	public void setView(String view) {
		this.view = view;
	}

	public String getFlow() {
		return flow;
	}

	public void setFlow(String flow) {
		this.flow = flow;
	}

	public String getComponent() {
		return component;
	}

	public void setComponent(String component) {
		this.component = component;
	}

}
