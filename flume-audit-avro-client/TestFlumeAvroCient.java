package test.bcp.core.audit.flume.client;

import junit.framework.TestCase;

import org.apache.commons.lang.SerializationUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bcp.core.audit.flume.client.FlumeAvroClient;
import com.bcp.core.audit.flume.client.dto.AuditInfo;
import com.bcp.core.audit.flume.client.dto.AuditLog3;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class TestFlumeAvroCient2 extends TestCase {

	private static final Logger	LOGGER		= LoggerFactory.getLogger(TestFlumeAvroCient2.class);

	private static final Gson	GSON		= new GsonBuilder().create();

	private static final String	HOST		= "localhost";
	private static final int	PORT		= 8000;
	private static final Long	EVENTID		= (long) 10999;
	private static final Long	ELAPSEDTIME	= (long) 10999;

	// 10.80.132.244
	// 10.80.197.58
	public void testApp() throws Exception {

		LOGGER.info("Start FlumeAvroCient");

		final long start = System.currentTimeMillis();
		long ellapsed = -1L;

		FlumeAvroClient audiInfoClient = new FlumeAvroClient();
		audiInfoClient.init(HOST, PORT);

		DateTime dateTime = new DateTime(DateTimeZone.UTC);
		// org.joda.time.format.DateTimeFormatter f =
		// DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		// DateTime dateTime = f.parseDateTime("2013-07-17T03:58:00.000Z");
		/*
		 * Object data = populateAuditInfo("ALEXANDER");
		 * Integer date = 2;
		 * Short time = 4;
		 * Short sec = 3;
		 * BigDecimal operationAmount;
		 * Double d = new Double("123.45678");
		 * operationAmount = BigDecimal.valueOf(d);
		 * BigDecimal exchangeRateAmount;
		 * Double d2 = new Double("123.45678");
		 * exchangeRateAmount = BigDecimal.valueOf(d);
		 * BigDecimal equivalentValue;
		 * Double d3 = new Double("123.45678");
		 * equivalentValue = BigDecimal.valueOf(d);
		 * Long currencyId = (long) 44;
		 * Long sourceProductType = (long) 55;
		 * Long targetProductType = (long) 66;
		 * Long targetCurrencyId = (long) 77;
		 */

		String log = "";
		//String cadenaJson = "{'key': 'value', 'key2': 'value2'}";
		String cadenaJson = "{\"Nombre\":\"SONY\",\"Apellido\":\"SerieE\",\"CodigoEstudiante\":200145,\"Promedio\":14.23,\"Colegio\":\"Google\",\"Universidad\":\"USMP\",\"AñoEgreso\":\"2011-01\",\"fecha nacimiento\":\"27-02-1987\""
				+ ",\"Marca\":\"Lenovo\",\"Celular\":true,\"Marca2\":\"Lenov2o2\",\"Marca3\":\"Lenovo33\",\"Marca4\":\"Lenovo4\",\"Marca5\":\"Lenovo55\",\"Marca6\":false, \"Telefono\": [\"555-0100\",\"555-0120\"]}";
		// Object fromjson = GSON.fromJson(cadenaJson,Object.class);
		// LOGGER.info(GSON.toJson(fromjson));

		AuditLog3 objAuditLog = populateAuditLog3("transactionId", "operationId", cadenaJson);

		for (int i = 0; i < 5; i++) {
			/*
			 * AuditLog objAuditLog = populateAuditLog("transactionId",
			 * "operationId", "serverNode", "sessionId", "sourceAppId",
			 * "channelId", "messageType", EVENTID,
			 * "cic", "idc", dateTime, "eventType", "component", "eventName",
			 * ELAPSEDTIME, "status", "{nombre:'alexander',apellido:'zelada'}",
			 * "dataRaw", "serverName", "serverVersion",
			 * "clientName", "clientLastName", "clientEmail",
			 * "alternativeEmail", "user", "clientSegment", "terminal",
			 * operationAmount, currencyId,
			 * "currencyName", exchangeRateAmount, equivalentValue,
			 * sourceProductType, "sourceProduct", targetProductType,
			 * "targetProduct", targetCurrencyId,
			 * "targetCurrencyName", "branchCod", "concept", "operationType",
			 * "operationNumber", "serviceCod", "depositorCod", "dataReq",
			 * "dataRes");
			 */
			log = Integer.valueOf(i) + " - " + GSON.toJson(objAuditLog);
			// LOGGER.info(log);
			byte[] auditInfoBytes = SerializationUtils.serialize(objAuditLog);
			audiInfoClient.sendDataToFlume(auditInfoBytes);
		}
		audiInfoClient.cleanUp();

		ellapsed = System.currentTimeMillis() - start;

		LOGGER.info("End FlumeAvroCient");
		LOGGER.info("Ellapsed time (millis): " + Long.valueOf(ellapsed));
	}

	public static AuditLog3 populateAuditLog3(String transactionId, String operationId, String data) {

		AuditLog3 auditLog = new AuditLog3();

		auditLog.setTransactionId(transactionId);
		auditLog.setOperationId(operationId);
		auditLog.setData(data);

		return auditLog;
	}

	/*
	 * public static AuditLog populateAuditLog(String transactionId, String
	 * operationId, String serverNode, String sessionId, String sourceAppId,
	 * String channelId, String messageType, Long eventId, String cic, String
	 * idc, DateTime dateTime, String eventType, String component,
	 * String eventName, Long elapsedTime, String status, String data, String
	 * dataRaw, String serverName, String serverVersion, String clientName,
	 * String clientLastName, String clientEmail, String alternativeEmail,
	 * String user, String clientSegment, String terminal, BigDecimal
	 * operationAmount,
	 * Long currencyId, String currencyName, BigDecimal exchangeRateAmount,
	 * BigDecimal equivalentValue, Long sourceProductType, String sourceProduct,
	 * Long targetProductType, String targetProduct, Long targetCurrencyId,
	 * String targetCurrencyName, String branchCod, String concept,
	 * String operationType, String operationNumber, String serviceCod, String
	 * depositorCod, String dataReq, String dataRes) {
	 * 
	 * AuditLog auditLog = new AuditLog();
	 * 
	 * auditLog.setTransactionId(transactionId);
	 * auditLog.setOperationId(operationId);
	 * auditLog.setServerNode(serverNode);
	 * auditLog.setSessionId(sessionId);
	 * auditLog.setSourceAppId(sourceAppId);
	 * auditLog.setChannelId(channelId);
	 * auditLog.setMessageType(messageType);
	 * auditLog.setEventId(eventId);
	 * auditLog.setCic(cic);
	 * auditLog.setIdc(idc);
	 * auditLog.setDateTime(dateTime);
	 * auditLog.setEventType(eventType);
	 * auditLog.setComponent(component);
	 * auditLog.setEventName(eventName);
	 * auditLog.setElapsedTime(elapsedTime);
	 * auditLog.setStatus(status);
	 * auditLog.setData(data);
	 * auditLog.setDataRaw(dataRaw);
	 * auditLog.setServerName(serverName);
	 * auditLog.setServerVersion(serverVersion);
	 * auditLog.setClientName(clientName);
	 * auditLog.setClientLastName(clientLastName);
	 * auditLog.setClientEmail(clientEmail);
	 * auditLog.setAlternativeEmail(alternativeEmail);
	 * auditLog.setUser(user);
	 * auditLog.setClientSegment(clientSegment);
	 * auditLog.setTerminal(terminal);
	 * auditLog.setOperationAmount(operationAmount);
	 * auditLog.setCurrencyId(targetCurrencyId);
	 * auditLog.setCurrencyName(targetCurrencyName);
	 * auditLog.setExchangeRateAmount(exchangeRateAmount);
	 * auditLog.setEquivalentValue(equivalentValue);
	 * auditLog.setSourceProductType(sourceProductType);
	 * auditLog.setSourceProduct(sourceProduct);
	 * auditLog.setTargetProductType(targetProductType);
	 * auditLog.setTargetProduct(targetProduct);
	 * auditLog.setTargetCurrencyId(targetCurrencyId);
	 * auditLog.setTargetCurrencyName(targetCurrencyName);
	 * auditLog.setBranchCod(branchCod);
	 * auditLog.setConcept(concept);
	 * auditLog.setOperationType(operationType);
	 * auditLog.setOperationNumber(operationNumber);
	 * auditLog.setServiceCod(serviceCod);
	 * auditLog.setDepositorCod(depositorCod);
	 * auditLog.setDataReq(dataReq);
	 * auditLog.setDataRes(dataRes);
	 * 
	 * return auditLog;
	 * 
	 * }
	 */
	public static AuditInfo populateAuditInfo(String nombre, String apellido) {
		AuditInfo audiInfo = new AuditInfo();

		audiInfo.setNombre(nombre);
		audiInfo.setApellido(apellido);

		return audiInfo;
	}

	/*
	 * public static AuditInfo populateAuditInfo(String nombre, String apellido,
	 * String edad, String fechaNacimiento) {
	 * 
	 * AuditInfo auditInfotest = new AuditInfo();
	 * 
	 * auditInfotest.setNombre(nombre);
	 * auditInfotest.setApellido(apellido);
	 * auditInfotest.setEdad(edad);
	 * auditInfotest.setFechanacimiento(fechaNacimiento);
	 * 
	 * return auditInfotest;
	 * }
	 */

	// 18876 millis 1M events without prettyprint and log disabled
	// 3792 millis without prettyprint and log disabled
	// 3993 millis with prettyprint and log disabled
	// 88000 millis with prettyprint and log enabled
	// 8.42 seconds with file
	// 4.95 seconds with memory
	// 311276 with nginx
	// 292975 without nginx
	// 278542 without nginx
	// Windows local machine without docker start 22:35:12.134Z end 22:36:23.098
	// Virtual machine with Docker start 23:03:23.197 end 23:04:13.788
	// 664002 millis Windows local machine from client
	// 46 seconds virtual machine local 4gb ram
	// 3 minutes y 46 seconds desarrollo mongo docker con 4gb ram
	// 6 minutes y 24 seconds desarrollo mongo docker memoria dinámica
	// 2 minutes y 53 seconds desarrollo mongo docker con 8gb ram
	// 2 minutes y 48 seconds desarrollo mongo docker con 8gb ram

	// flume+hdfs 224026 millis
	// nginx flume+hdfs 272975 millis
	// nginx flume+hdfs modificado 344901
}