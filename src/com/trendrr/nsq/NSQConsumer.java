package com.trendrr.nsq;
/**
 * 
 */

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
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
public class NSQConsumer extends NSQProducer {

	protected static Log log = LogFactory.getLog(NSQConsumer.class);
	
	NSQLookup lookup;
	String topic = null;
	String channel = null;
	MessageCallback callback;
	
    
	public NSQConsumer(NSQLookup lookup, String topic, String channel, MessageCallback callback) {
		super(1);
		this.lookup = lookup;
	    this.topic = topic;
	    this.channel = channel;
	    this.callback = callback;
	}
	
	/**
	 * 
	 * Connects and subscribes to the requested topic and channel.
	 * 
	 * safe to call repeatedly for node discovery.
	 */
	protected synchronized void connect() {
		if (this.bootstrap == null) {
			//create default bootstrap
			this.setNettyExecutors(Executors.newCachedThreadPool(),
					Executors.newCachedThreadPool());
		}
		if (this.topic == null) {
			log.warn("Subscribed topic is null, cant connect");
			return;
		}
		DynMap mp = lookup.lookup(this.topic);
		System.out.println(mp.toJSONString());
		for (DynMap node : mp.getListOrEmpty(DynMap.class, "data.producers")) {
			
			String key = node.getString("address") + ":" + node.getInteger("tcp_port");
			if (this.connections.containsKey(key)) {
				//already connected to this one.
				continue;
			}
			
			Connection conn = this.createConnection(node.getString("address"), node.getInteger("tcp_port"));
			conn.setCallback(callback);
			/*
			 * subscribe
			 */
			conn.command(NSQCommand.instance("SUB " + topic + " " + this.channel));
			conn.command(NSQCommand.instance("RDY " + conn.getMessagesPerBatch()));
		}
		
		this.cleanupOldConnections();
	}
}
