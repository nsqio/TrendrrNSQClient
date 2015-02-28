package com.trendrr.nsq.exceptions;

public class NoConnectionsException extends Exception {
	private static final long serialVersionUID = -7902210058291470928L;

	public NoConnectionsException(String message, Throwable cause) {
		super(message, cause);
	}
}
