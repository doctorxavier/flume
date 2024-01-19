package com.bcp.core.audit.flume.interceptor.audit.hdfs.parser.csv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.CellProcessorAdaptor;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.util.CsvContext;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class AuditInfoCellProcessor extends CellProcessorAdaptor {

	private static final Logger	LOGGER	= LoggerFactory.getLogger(AuditInfoCellProcessor.class);

	private static final Gson	GSON	= new GsonBuilder().create();

	public AuditInfoCellProcessor() {
		super();
	}

	public AuditInfoCellProcessor(CellProcessor next) {
		super(next);
	}

	public Object execute(Object value, CsvContext context) {

		final String json = GSON.toJson(value);

		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace(json);
		}

		return next.execute(GSON.toJson(value), context);
	}

}
