package com.trendrr.nsq.frames;

import java.io.UnsupportedEncodingException;

import org.apache.logging.log4j.LogManager;

public class ResponseFrame extends NSQFrame {
	
	public String getMessage() {
		try {
			return new String(getData(), "utf8");
		} catch (UnsupportedEncodingException e) {
			LogManager.getLogger(this).error("Caught", e);
		}
		return null;
	}
	
	public String toString() {
		return "RESPONSE: " + this.getMessage();
	}
}
