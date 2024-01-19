package com.bcp.core.audit.flume.interceptor.behavior.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bcp.core.audit.flume.interceptor.behavior.hdfs.parser.csv.BehaivorInfoCvsParser;

public class MessageInterceptorHDFS implements Interceptor {

	private static final Logger	LOGGER	= LoggerFactory.getLogger(MessageInterceptorHDFS.class);

	// @Override
	public void initialize() {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("initialize MessageInterceptor ...");
		}
	}

	// @Override
	public Event intercept(Event event) {

		if (LOGGER.isDebugEnabled() && !LOGGER.isTraceEnabled()) {
			LOGGER.debug("intercept event MessageInterceptor ...");
		}
		if (event != null) {
			if (LOGGER.isDebugEnabled() && !LOGGER.isTraceEnabled()) {
				LOGGER.debug("intercepted event MessageInterceptor ...");
			}
			try {

				final Object object = SerializationUtils.deserialize(event.getBody());
				final String csv = BehaivorInfoCvsParser.writeCsv(object);

				if (LOGGER.isTraceEnabled()) {
					LOGGER.trace(csv);
				}

				event.setBody(csv.getBytes("UTF-8"));
			} catch (IOException e) {
				LOGGER.error(ExceptionUtils.getMessage(e));
			}
		}
		return event;
	}

	// @Override
	public List<Event> intercept(List<Event> events) {
		List<Event> eventsParsed = new ArrayList<Event>(0);
		for (Event event : events) {
			if (intercept(event) != null) {
				eventsParsed.add(event);
			}
		}
		return eventsParsed;
	}

	// @Override
	public void close() {

	}

	public static class Builder implements Interceptor.Builder {

		// @Override
		public void configure(Context context) {

		}

		// @Override
		public Interceptor build() {
			return new MessageInterceptorHDFS();
		}

	}

}
