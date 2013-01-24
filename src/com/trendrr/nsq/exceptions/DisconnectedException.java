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
public class DisconnectedException extends Exception {

	protected static Logger log = LoggerFactory.getLogger(DisconnectedException.class);
	
	public DisconnectedException(String message, Throwable cause) {
		super(message, cause);
	}
}
