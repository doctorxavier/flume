package com.bcp.core.audit.flume.client.exception;

public class AuditLogException extends Exception {

	public static final String	U0001				= "Object not serializable to AuditLog.";
	public static final String	U0002				= "Avro client not initialized.";

	private static final long	serialVersionUID	= 1L;

	public AuditLogException(Exception e) {
		super(e);
	}

	public AuditLogException(final String message) {
		super(message);
	}

}
