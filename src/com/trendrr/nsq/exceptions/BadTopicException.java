/**
 * 
 */
package com.trendrr.nsq.exceptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Dustin Norlander
 * @created Jan 22, 2013
 * 
 */
public class BadTopicException extends Exception {

	protected static Logger log = LoggerFactory.getLogger(BadTopicException.class);
	
	public BadTopicException(String message) {
		super(message);
	}
}
