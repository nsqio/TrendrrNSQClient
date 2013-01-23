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
public class BadMessageException extends Exception {

	protected static Log log = LogFactory.getLog(BadMessageException.class);
	
	public BadMessageException(String message) {
		super(message);
	}
}
