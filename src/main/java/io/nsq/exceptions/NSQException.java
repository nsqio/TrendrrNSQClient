package io.nsq.exceptions;

public class NSQException extends Exception {

    public NSQException(String message) {
        super(message);
    }

    public NSQException(String message, Throwable cause) {
        super(message, cause);
    }
}
