package com.bcp.core.audit.flume.interceptor.audit.hdfs.parser.csv;

import java.io.IOException;
import java.io.StringWriter;

import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.cellprocessor.joda.FmtDateTime;
import org.supercsv.io.CsvBeanWriter;
import org.supercsv.io.ICsvBeanWriter;
import org.supercsv.prefs.CsvPreference;


public final class AuditInfoCvsParser {

	private static final CsvPreference		PIPE_DELIMITED	= new CsvPreference.Builder('"', '|', "").build();
	private static final String[]			HEADER			= new String[] {"id", "cic", "idc", "eventType", "datetime", "date", "time", "sec", "millisec", "eventName",
			"transactionId", "operationCod", "operationTypeCod", "errorCod", "clientId", "sessionId", "sessionToken", "clientIp", "clientData","serverNode",
			"elapsedTime","dataType", "dataFormat","data", "dataToken",};
	
	private static final CellProcessor[]	CELL_PROCESSORS	= getProcessors();

	private AuditInfoCvsParser() {

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
				
				// new FmtDateTime("yyyyMMdd"),
				// new FmtDateTime("HHmmss"),
				new Optional(), 
				new Optional(), 
				new Optional(), 
				new Optional(),
				new Optional(new FmtDateTime("yyyy-MM-dd'T'HH:mm:ss.SSSZ")),
				new Optional(), 
				new Optional(), 
				new Optional(), 
				new Optional(), 
				new Optional(), 
				new Optional(), 
				new Optional(),
				new Optional(), 
				new Optional(),
				new Optional(), 
				new Optional(),
				new Optional(),
				new Optional(),
				new Optional(),
				new Optional(),
				new Optional(),
				new Optional(),
				new Optional(),
				new AuditInfoCellProcessor(),
				new Optional(),
		};
		return processors;

	}

}
