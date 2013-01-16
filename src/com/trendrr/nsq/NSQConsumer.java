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
public class NSQConsumer {

	protected static Log log = LogFactory.getLog(NSQConsumer.class);
	
	HashMap<String, Connection> connections = new HashMap<String, Connection>();
	
	NSQLookup lookup;
	
	String topic = null;
	String channel = null;
	MessageCallback callback;
	
	// Configure the client.
    ClientBootstrap bootstrap = null;
    public static final int MAGIC_PROTOCOL_VERSION = 538990130; //uhh, wtf is this?
    
	public NSQConsumer(NSQLookup lookup, String topic, String channel, MessageCallback callback) {
		this.lookup = lookup;
	    this.topic = topic;
	    this.channel = channel;
	    this.callback = callback;
	}
	
	/**
	 * immediately starts consuming.
	 */
	public void start() {
		this.connect();
	}
	/**
	 * use this if you want to specify your own netty executors. by default will use
	 * 
	 * Executors.newCachedThreadPool()
	 * 
	 * @param boss
	 * @param worker
	 */
	public synchronized void setNettyExecutors(Executor boss, Executor worker) {
		if (this.bootstrap != null) {
			this.bootstrap.releaseExternalResources();
		}
		this.bootstrap = new ClientBootstrap(
	            new NioClientSocketChannelFactory(
	                    boss,
	                    worker));
		bootstrap.setPipelineFactory(new NSQPipeline());
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
		for (DynMap node : mp.getList(DynMap.class, "data.producers")) {
			
			String key = node.getString("address") + ":" + node.getInteger("tcp_port");
			if (this.connections.containsKey(key)) {
				//already connected to this one.
				continue;
			}
			
			// Start the connection attempt.
	        ChannelFuture future = bootstrap.connect(new InetSocketAddress(node.getString("address"), node.getInteger("tcp_port")));

	        // Wait until the connection attempt succeeds or fails.
	        Channel channel = future.awaitUninterruptibly().getChannel();
	        if (!future.isSuccess()) {
	            log.error("Caught", future.getCause());
	            continue;
	        }
	        Connection conn = new Connection(channel, this);
	        ChannelBuffer buf = ChannelBuffers.dynamicBuffer();
	        buf.writeInt(MAGIC_PROTOCOL_VERSION);
	        channel.write(buf);
			this.connections.put(key, conn);
			conn.setCallback(callback);
			/*
			 * subscribe
			 */
			conn.command(NSQCommand.instance("SUB " + topic + " " + this.channel));
			conn.command(NSQCommand.instance("RDY " + conn.getMessagesPerBatch()));
		}
		
		this.cleanupOldConnections();
	}
	
	/**
	 * will run through and remove any connections that have not recieved a ping in the last 2 minutes.
	 */
	public synchronized void cleanupOldConnections() {
		ArrayList<Connection> cons = new ArrayList<Connection>();
		
		Date cutoff = Timeframe.MINUTES.add(new Date(), -2);
		
		for (String k : this.connections.keySet()) {
			Connection c = this.connections.get(k);
			if (cutoff.after(c.getLastHeartbeat())) {
				cons.add(c);
			}
		}
		for (Connection c: cons) {
			c.close();
		}
	}
	
	synchronized void disconnected(Connection connection) {
		for (String k : this.connections.keySet()) {
			if (this.connections.get(k) == connection) {
				this.connections.remove(k);
			}
		}
	}
	
	public synchronized void close() {
		for (Connection c : connections.values()) {
			c.close();
		}
	}
}
