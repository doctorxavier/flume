package test.bcp.core.audit.flume.interceptor.behavior.hdfs;

import org.apache.commons.lang.SerializationUtils;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.InterceptorBuilderFactory;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bcp.core.audit.flume.interceptor.behavior.hdfs.dto.BehaivorLog;

public class TestMessageInterceptorHDFS {

	private static final Logger	LOGGER		= LoggerFactory.getLogger(TestMessageInterceptorHDFS.class);

	@Test
	public void interceptHDFS() throws Exception {

		org.joda.time.format.DateTimeFormatter f = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
		DateTime hour = f.parseDateTime("2012-01-10 23:13:26");
		DateTime date = f.parseDateTime("2012-01-10 23:13:26");
		DateTime dateTime = f.parseDateTime("2012-01-10 23:13:26");
		DateTime timestamp = f.parseDateTime("2012-01-10 23:13:26");
		
		Short millisecond = 4;
		Short second = 3;

		BehaivorLog objBehLog = populateBehLog(hour, second, millisecond, dateTime, timestamp, "node", "clientIp", "clientInfo", "referer", "cic", "idc",
				"sessionId", "sessionToken", "requestUrl", "view", "flow", "component", date);

		byte[] auditInfoBytes = SerializationUtils.serialize(objBehLog);
		Interceptor.Builder builder = InterceptorBuilderFactory.newInstance("com.bcp.core.audit.flume.interceptor.behavior.hdfs.MessageInterceptorHDFS$Builder");
		Interceptor interceptor = builder.build();
		Event eventBeforeIntercept = EventBuilder.withBody(auditInfoBytes);
		Event eventAfterIntercept = interceptor.intercept(eventBeforeIntercept);
		String csv = new String(eventAfterIntercept.getBody());
		LOGGER.info(csv);
	}

	public static BehaivorLog populateBehLog(DateTime hour, Short second, Short milisecond, DateTime dateTime, DateTime timestamp, String node,
			String clientIp, String clientInfo, String referer, String cic, String idc, String sessionId, String sessionToken, String requestUrl, String view,
			String flow, String component, DateTime date) {

		BehaivorLog behLog = new BehaivorLog();

		behLog.setHour(hour);
		behLog.setSecond(second);
		behLog.setMillisecond(milisecond);
		behLog.setDateTime(dateTime);
		behLog.setTimestamp(dateTime);
		behLog.setNode(node);
		behLog.setClientIp(clientIp);
		behLog.setClientInfo(clientInfo);
		behLog.setReferer(referer);
		behLog.setCic(cic);
		behLog.setIdc(idc);
		behLog.setSessionId(sessionId);
		behLog.setSessionToken(sessionToken);
		behLog.setRequestUrl(requestUrl);
		behLog.setView(view);
		behLog.setFlow(flow);
		behLog.setComponent(component);
		behLog.setDate(date);

		return behLog;

	}

}
