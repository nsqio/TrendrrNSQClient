/**
 * 
 */
package com.trendrr.nsq.frames;

import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * @author Dustin Norlander
 * @created Jan 14, 2013
 * 
 */
public class ErrorFrame extends NSQFrame {

	protected static Log log = LogFactory.getLog(ErrorFrame.class);
	
	public ErrorFrame() {
		this.frameId = 1;
	}
	
	public String getErrorMessage() {
		try {
			return new String(this.data, "utf8");
		} catch (UnsupportedEncodingException e) {
			log.error("Caught", e);
		}
		return null;
	}
	
	public String toString() {
		return this.getErrorMessage();
	}
}
