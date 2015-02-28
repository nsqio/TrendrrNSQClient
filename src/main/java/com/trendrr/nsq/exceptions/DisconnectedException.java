package com.trendrr.nsq.exceptions;

public class DisconnectedException extends Exception {
	private static final long serialVersionUID = 6014743379707601555L;
	
	public DisconnectedException(String message, Throwable cause) {
		super(message, cause);
	}
}
