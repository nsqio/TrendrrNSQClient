/**
 * 
 */
package com.trendrr.nsq;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.trendrr.nsq.frames.ErrorFrame;
import com.trendrr.nsq.frames.ResponseFrame;
import com.trendrr.nsq.netty.NSQPipeline;
import com.trendrr.oss.DynMap;
import com.trendrr.oss.Timeframe;


/**
 * @author Dustin Norlander
 * @created Jan 15, 2013
 * 
 */
public class NSQProducer {

	/**
	 * Protocol version sent to nsqd on initial connect
	 */
	public static byte[] MAGIC_PROTOCOL_VERSION = "  V2".getBytes();
    public static long LOOKUP_PERIOD = 60*1000; //how often to recheck for new nodes (and clean up non responsive nodes)
    
    /**
     * The maximum number of messages we can send without recieving an OK from the server.
     */
    int maxInflight = 100;
    
	protected static Log log = LogFactory.getLog(NSQProducer.class);
	
	protected HashMap<String, Connection> connections = new HashMap<String, Connection>();
	protected ArrayList<String> connectionKeys = new ArrayList<String>(); //keys to roundrobin connections.
	
	protected AtomicLong produced = new AtomicLong(0l);
	
	Semaphore inflight;
	
	// Configure the client.
    protected ClientBootstrap bootstrap = null;
    protected Timer timer = null;
    
    /**
     * list of the requested connection addresses in the form [addr,port]
     */
    protected ArrayList<String[]> addresses = new ArrayList<String[]>();
    
    
    /**
     * creates a new producer.
     * 
     *   
     * @param maxInFlight the max number of messages that can be in route without recieving the associated OK.  higher will give more throughput but on failure you will not know which message 
     * was the cause.  1 would be total synchronous operation.
     */
	public NSQProducer(int maxInFlight) {
		this.maxInflight = maxInFlight;
		this.inflight = new Semaphore(maxInFlight);
	}
	
	/**
	 * adds new address to the connection queue.
	 * 
	 * @param address
	 * @param port
	 * @return
	 */
	public NSQProducer addAddress(String address, int port) {
		addresses.add(new String[]{address, Integer.toString(port)});
		return this;
	}
	
	/**
	 * @param topic
	 * @param message
	 */
	public void produce(String topic, byte[] message) {
		
		try {
			this.inflight.acquire();
		} catch (InterruptedException e) {
			log.error("Caught", e);
		}
		
		/*
		 * Roundrobin the connections if we have multiple.. 
		 */
		int ind = (int)(this.produced.incrementAndGet() % this.connectionKeys.size());
		Connection conn = null;
		try {
			conn = this.connections.get(this.connectionKeys.get(ind));
		} catch (Exception x) {
			//could happen array index out of bounds, since we
			//arent being atomic.  nbd, we will just start at ind-1;
			conn = this.connections.get(this.connectionKeys.get(this.connectionKeys.size()-1));
		}
		
		if (conn == null) {
			//alright alright, roundrobin didn't work
			log.warn("Roundrobin failed for unknown reason, help help");
			conn = this.connections.values().iterator().next();
		}
		
		NSQCommand command = NSQCommand.instance("PUB " + topic, message);
		conn.command(command);
	}
	
	/**
	 * connects, ready to produce.
	 */
	public synchronized void start() {
		this.connect();

		if (timer != null) {
			timer.cancel();
		}
		timer = new Timer();
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				connect();
			}
		}, LOOKUP_PERIOD, LOOKUP_PERIOD);
		
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
	 * Creates a new connection object.
	 * 
	 * Handles connection and sending magic protocol 
	 * @param address
	 * @param port
	 * @return
	 */
	protected Connection createConnection(String address, int port) {
		
		// Start the connection attempt.
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(address, port));

        // Wait until the connection attempt succeeds or fails.
        Channel channel = future.awaitUninterruptibly().getChannel();
        if (!future.isSuccess()) {
            log.error("Caught", future.getCause());
            return null;
        }
        Connection conn = new Connection(channel, this);
        ChannelBuffer buf = ChannelBuffers.dynamicBuffer();
        buf.writeBytes(MAGIC_PROTOCOL_VERSION);
        channel.write(buf);
        return conn;
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
		for (String[] addr : this.addresses ) {
			String key = addr[0] + ":" + addr[1];
			if (this.connections.containsKey(key)) {
				//already connected to this one.
				continue;
			}
			
			Connection conn = this.createConnection(addr[0], Integer.parseInt(addr[1]));
			this.connections.put(key, conn);
			this.connectionKeys.add(key);
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
				this.connectionKeys.remove(k);
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
				this.connectionKeys.remove(k);
				System.out.println("removing: " + k);
			}
			//clear out the semiphores
			//this is bad, but we don't know how many are in play for a 
			//specific connection.
			this.inflight.drainPermits();
		}
	}
	
	public synchronized void close() {
		for (Connection c : connections.values()) {
			c.close();
		}
	}
	
	public void _incomingResponse(ResponseFrame frame, Connection con) {
		if ("OK".equals(frame.getMessage())) {
			this.inflight.release();
		}
	}
	
	public void _incomingError(ErrorFrame frame, Connection con) {
		log.error("Caught : " + frame);
		con.close();
	}
}
