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
public class DisconnectedException extends Exception {

	protected static Log log = LogFactory.getLog(DisconnectedException.class);
	
	public DisconnectedException(String message, Throwable cause) {
		super(message, cause);
	}
}
