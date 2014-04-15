/**
 * 
 */
package com.trendrr.nsq;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.trendrr.nsq.exceptions.NoConnectionsException;
import com.trendrr.nsq.netty.NSQPipeline;


/**
 * Base class for producer and consumer
 * 
 * @author Dustin Norlander
 * @created Jan 22, 2013
 * 
 */
public abstract class AbstractNSQClient {

	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractNSQClient.class);

    private static final int CLEAN_UP_FREQUENCY = 1000 * 60 * 2;

	/**
	 * Protocol version sent to nsqd on initial connect
	 */
	private static byte[] MAGIC_PROTOCOL_VERSION = "  V2".getBytes();
    private static long LOOKUP_PERIOD = 60*1000; //how often to recheck for new nodes (and clean up non responsive nodes)
    
    
	private final Connections connections = new Connections();
	// Configure the client.
    private ClientBootstrap bootstrap = null;
    private Timer timer = null;

    
    //this executor is where the callback code is handled
    protected Executor executor = Executors.newSingleThreadExecutor();
	
    /**
	 * connects, ready to produce.
	 */
	public synchronized void start() {
		this.connect();

		if (timer != null) {
			timer.cancel();
		}
		timer = new Timer();

        //TODO Use a scheduled executor service instead of a timer here
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				connect();
			}
		}, LOOKUP_PERIOD, LOOKUP_PERIOD);
		
	}
	
	/**
	 * Should return a list of all the addresses that we should be currently connected to. 
	 * @return
	 */
	public abstract List<ConnectionAddress> lookupAddresses();
	
	/**
	 * this is the executor where the callbacks happen.  default is a new cached threadpool.
	 * @param executor
	 */
	public synchronized void setExecutor(Executor executor) {
		this.executor = executor;
	}
	
	public Executor getExecutor() {
		return this.executor;	
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
            LOGGER.error("Unable to create connection, caught: ", future.getCause());
            return null;
        }

        LOGGER.warn("Creating connection: " + address + " : " + port);
        Connection conn = new Connection(address, port, channel, this);
        ChannelBuffer buf = ChannelBuffers.dynamicBuffer();
        buf.writeBytes(MAGIC_PROTOCOL_VERSION);
        channel.write(buf);
        
        //indentify
		try {
			String identJson = "{" +
					"\"short_id\":\"" + InetAddress.getLocalHost().getHostName() + "\"" +
					"," +
					"\"long_id\":\"" + InetAddress.getLocalHost().getCanonicalHostName() + "\"" +
					"}";
			NSQCommand ident = NSQCommand.instance("IDENTIFY", identJson.getBytes());
			conn.command(ident);
			
		} catch (UnknownHostException e) {
			LOGGER.error("Caught", e);
		}
        		
        		
       
        
        
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
		
		List<ConnectionAddress> addresses = this.lookupAddresses();
		
		
		for (ConnectionAddress addr : addresses ) {
			int num = addr.getPoolsize() - connections.connectionSize(addr.getHost(), addr.getPort());
			for (int i=0; i < num; i++) {
			    Connection conn = createConnection(addr.getHost(), addr.getPort());
				connections.addConnection(conn);

			}
			//TODO: handle negative num? (i.e. if user lowered the poolsize we should kill some connections)
		}
		this.cleanupOldConnections();
	}
	
	/**
	 * will run through and remove any connections that have not recieved a ping in the last 2 minutes.
	 */
	public synchronized void cleanupOldConnections() {
        Date cutoff = new Date(new Date().getTime() - CLEAN_UP_FREQUENCY);
		try {
			for (Connection c : this.connections.getConnections()) {
				if (cutoff.after(c.getLastHeartbeat())) {
					LOGGER.warn("Removing dead connection [host={}, port={}]", c.getHost(), c.getPort());
					c.close();
					connections.remove(c);
				}
			}
		} catch (NoConnectionsException e) {
			//ignore
		}
	}

    public Connections getConnections(){
        return connections;
    }


	/**
	 * for internal use.  called when a connection is disconnected
	 * @param connection
	 */
	public synchronized void _disconnected(Connection connection) {
		LOGGER.warn("Client disconnected [connection={}]", connection);
		this.connections.remove(connection);
	}
	
	public synchronized void close() {
		this.timer.cancel();
		this.connections.close();
		this.bootstrap.releaseExternalResources();
		
	}
}
