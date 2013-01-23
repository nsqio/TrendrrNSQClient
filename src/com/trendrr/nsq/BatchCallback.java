/**
 * 
 */
package com.trendrr.nsq;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * @author Dustin Norlander
 * @created Jan 22, 2013
 * 
 */
public interface BatchCallback {

	public void batchSuccess(String topic, int num);
	public void batchError(Exception ex, String topic, List<byte[]> messages);
}
