package com.trendrr.nsq;

/**
 * 
 */

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import com.trendrr.nsq.frames.MessageFrame;
import com.trendrr.nsq.frames.NSQFrame;
import com.trendrr.nsq.frames.ResponseFrame;
import com.trendrr.oss.exceptions.TrendrrDisconnectedException;




/**
 * @author Dustin Norlander
 * @created Jan 14, 2013
 * 
 */
public class Connection {

	protected static Log log = LogFactory.getLog(Connection.class);
	
	Channel channel;
	int heartbeats = 0;
	Date lastHeartbeat = new Date();
	
	MessageCallback callback = null;
	int totalMessages = 0;
	int messagesPerBatch = 200;
	
	NSQReader client = null;
	
	public Connection(Channel channel, NSQReader client) {
		this.channel = channel;
		this.channel.setAttachment(this);
		this.client = client;
	}
	
	public int getMessagesPerBatch() {
		return messagesPerBatch;
	}


	public void setMessagesPerBatch(int messagesPerBatch) {
		this.messagesPerBatch = messagesPerBatch;
	}


	
	
	
	public void incoming(NSQFrame frame) {
		if (frame instanceof ResponseFrame) {
			if ("_heartbeat_".equals(((ResponseFrame) frame).getMessage())) {
				this.heartbeat();
				return;
			}	
		}
		if (frame instanceof MessageFrame) {
			this.totalMessages++;
			if (totalMessages % messagesPerBatch > (messagesPerBatch/2)) {
				//request some more!
				this.command(NSQCommand.instance("RDY " + this.messagesPerBatch));
			}
			
			
			NSQMessage message = new NSQMessage();
			message.setAttempts(((MessageFrame) frame).getAttempts());
			message.setConnection(this);
			message.setId(((MessageFrame) frame).getMessageId());
			message.setMessage(((MessageFrame) frame).getMessageBody());
			message.setTimestamp(new Date(((MessageFrame) frame).getTimestamp()));
			if (this.callback == null) {
				log.warn("NO CAllback, dropping message: " + message);
			} else {
				this.callback.message(message);
			}
			return;
		}
		
		
		//incoming message, give back to whoever needs it.
		System.out.println("GOT FRAME! " + frame);
	}
	
	
	void heartbeat() {
		System.out.println("HEARTBEAT!");
		this.heartbeats++;
		this.lastHeartbeat = new Date();
		//send NOP here.
		this.command(NSQCommand.instance("NOP"));
	}
	
	void disconnected() {
		//clean up anything that needs cleaning up.
		this.client.disconnected(this);
	} 
	
	public int getHeartbeats() {
		return heartbeats;
	}

	public Date getLastHeartbeat() {
		return lastHeartbeat;
	}

	public int getTotalMessages() {
		return totalMessages;
	}

	public MessageCallback getCallback() {
		return callback;
	}


	public void setCallback(MessageCallback callback) {
		this.callback = callback;
	}


	public void close() {
		try {
			channel.close().await(10000);
		} catch (Exception x) {
			log.error("Caught", x);
		}
		this.disconnected();
	}
	
	public ChannelFuture command(NSQCommand command) {
		return this.channel.write(command);
	}
}
