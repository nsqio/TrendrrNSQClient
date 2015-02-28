package com.trendrr.nsq.exceptions;

public class BadMessageException extends Exception {
	private static final long serialVersionUID = 6020565503860543141L;

	public BadMessageException(String message) {
		super(message);
	}
}
