/**
 * 
 */
package com.trendrr.nsq.exceptions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * @author Dustin Norlander
 * @created Jan 22, 2013
 * 
 */
public class BadTopicException extends Exception {

	protected static Log log = LogFactory.getLog(BadTopicException.class);
	
	public BadTopicException(String message) {
		super(message);
	}
}
