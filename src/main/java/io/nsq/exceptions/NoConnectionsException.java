package io.nsq.exceptions;

public class NoConnectionsException extends NSQException {

	public NoConnectionsException(String message, Throwable cause) {
		super(message, cause);
	}
}
