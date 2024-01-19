package com.bcp.core.audit.flume.interceptor.behavior.hdfs.parser.csv;

import java.io.IOException;
import java.io.StringWriter;

import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.cellprocessor.joda.FmtDateTime;
import org.supercsv.io.CsvBeanWriter;
import org.supercsv.io.ICsvBeanWriter;
import org.supercsv.prefs.CsvPreference;

public final class BehaivorInfoCvsParser {

	private static final CsvPreference		PIPE_DELIMITED	= new CsvPreference.Builder('"', '|', "").build();

	private static final String[]			HEADER			= new String[] {"hour","second", "millisecond", "dateTime", "timestamp", "node", "clientIp", "clientInfo",
			"referer", "cic", "idc", "sessionId", "sessionToken", "requestUrl", "view", "flow", "component","date",};

	private static final CellProcessor[]	CELL_PROCESSORS	= getProcessors();

	private BehaivorInfoCvsParser() {

	}

	public static String writeCsv(Object auditinfo) throws IOException {
		ICsvBeanWriter beanWriter = null;
		StringWriter stringWriter = new StringWriter();

		try {
			beanWriter = new CsvBeanWriter(stringWriter, PIPE_DELIMITED);

			beanWriter.write(auditinfo, HEADER, CELL_PROCESSORS);

		} catch (IOException e) {
			throw new IOException(e);
		} finally {
			if (beanWriter != null) {
				beanWriter.close();
			}
		}

		String csv = stringWriter.toString();
		stringWriter.close();

		return csv;

	}

	private static CellProcessor[] getProcessors() {

		final CellProcessor[] processors = new CellProcessor[] {

		new FmtDateTime("yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
		new NotNull(),
		new NotNull(),
		new FmtDateTime("yyyy-MM-dd'T'HH:mm:ss.SSSZ"), 
		new FmtDateTime("yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
		new NotNull(),
		new NotNull(),
		new NotNull(),
		new NotNull(), 
		new NotNull(), 
		new NotNull(), 
		new NotNull(), 
		new NotNull(), 
		new NotNull(), 
		new NotNull(),
		new NotNull(), 
		new NotNull(),
		new FmtDateTime("yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
		};
		return processors;
		
	}

}
