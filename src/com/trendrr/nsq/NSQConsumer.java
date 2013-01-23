package com.trendrr.nsq;
/**
 * 
 */

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.trendrr.oss.DynMap;
import com.trendrr.oss.Timeframe;



/**
 * @author Dustin Norlander
 * @created Jan 14, 2013
 * 
 */
public class NSQConsumer extends AbstractNSQClient {

	protected static Log log = LogFactory.getLog(NSQConsumer.class);
	
	NSQLookup lookup;
	String topic = null;
	String channel = null;
	MessageCallback callback;
	
    
	public NSQConsumer(NSQLookup lookup, String topic, String channel, MessageCallback callback) {
		this.lookup = lookup;
	    this.topic = topic;
	    this.channel = channel;
	    this.callback = callback;
	}
	
	@Override
	protected Connection createConnection(String address, int port) {
		Connection conn = super.createConnection(address, port);
		
		conn.setCallback(callback);
		/*
		 * subscribe
		 */
		conn.command(NSQCommand.instance("SUB " + topic + " " + this.channel));
		conn.command(NSQCommand.instance("RDY " + conn.getMessagesPerBatch()));
		return conn;
		
	}
	/* (non-Javadoc)
	 * @see com.trendrr.nsq.AbstractNSQClient#lookupAddresses()
	 */
	@Override
	public List<ConnectionAddress> lookupAddresses() {
		return lookup.lookup(topic);
	}
}
