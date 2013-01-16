/**
 * 
 */
package com.trendrr.nsq;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.trendrr.oss.StringHelper;


/**
 * @author Dustin Norlander
 * @created Jan 15, 2013
 * 
 */
public class NSQMessage {

	protected static Log log = LogFactory.getLog(NSQMessage.class);
	protected byte[] id;
	protected int attempts;
	protected Date timestamp;
	protected byte[] message;
	protected Connection connection;
	
	/**
	 * Finished processing this message, let nsq know so it doesnt get reprocessed.
	 */
	public void finished() {
		this.connection.command(NSQCommand.instance("FIN " + StringHelper.toHex(id)));
	}
	
	public Connection getConnection() {
		return connection;
	}
	public void setConnection(Connection connection) {
		this.connection = connection;
	}
	public byte[] getId() {
		return id;
	}
	public void setId(byte[] id) {
		this.id = id;
	}
	public int getAttempts() {
		return attempts;
	}
	public void setAttempts(int attempts) {
		this.attempts = attempts;
	}
	public Date getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}
	public byte[] getMessage() {
		return message;
	}
	public void setMessage(byte[] message) {
		this.message = message;
	}

	
	
}
