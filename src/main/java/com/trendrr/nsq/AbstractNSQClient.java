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
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

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

    protected static Logger log = LoggerFactory.getLogger(AbstractNSQClient.class);


    /**
     * Protocol version sent to nsqd on initial connect
     */
    public static byte[] MAGIC_PROTOCOL_VERSION = "  V2".getBytes();

    private int messagesPerBatch = 200;
    private long lookupPeriod = 60 * 1000; // how often to recheck for new nodes (and clean up non responsive nodes)


    Connections connections = new Connections();
    // Configure the client.
    protected ClientBootstrap bootstrap = null;
    protected Timer timer = null;

    //this executor is where the callback code is handled
    protected ExecutorService executor = Executors.newSingleThreadExecutor();

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
                try {
                    connect();
                } catch (Throwable t) {
                    log.error("Error in periodic `connect` call", t);
                }
            }
        }, lookupPeriod, lookupPeriod);
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
    public synchronized void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public ExecutorService getExecutor() {
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
        this.bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(boss, worker));
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
        log.info("Creating connection: " + address + " : " + port);
        Connection conn = new Connection(address, port, channel, this);
        conn.setMessagesPerBatch(this.messagesPerBatch);

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
            log.error("Caught", e);
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
            int num = addr.getPoolsize() - this.connections.connectionSize(addr.getHost(), addr.getPort());
            for (int i=0; i < num; i++) {
                Connection conn = this.createConnection(addr.getHost(), addr.getPort());
                this.connections.addConnection(conn);
            }
            //TODO: handle negative num? (i.e. if user lowered the poolsize we should kill some connections)
        }
        this.cleanupOldConnections();
        this.adjustPerConnectionMessagePerBatch();
    }

    /**
     * will run through and remove any connections that have not recieved a ping in the last 2 minutes.
     */
    public synchronized void cleanupOldConnections() {
        Date cutoff = new Date(new Date().getTime() - (1000*60*2));
        try {
            for (Connection c : this.connections.getConnections()) {
                if (cutoff.after(c.getLastHeartbeat())) {
                    log.warn("Removing dead connection: " + c.getHost() + ":" + c.getPort());
                    c.close();
                    connections.remove(c);
                }
            }
        } catch (NoConnectionsException e) {
            //ignore
        }
    }

    /**
     * Adjust max in flight depending on the number of connections
     */
    public synchronized void adjustPerConnectionMessagePerBatch() {
        try {
            int numConnections = this.connections.size();
            if (numConnections == 0) {
                log.warn("connect: No connections; skipping max-in-flight adjustment");
            } else {
                int perConnectionInFlight;
                if (this.messagesPerBatch < numConnections) {
                    perConnectionInFlight = 1;
                } else {
                    perConnectionInFlight = this.messagesPerBatch / numConnections;
                }
                for (Connection conn: this.connections.getConnections()) {
                    conn.setMessagesPerBatch(perConnectionInFlight);
                }
            }
        } catch (NoConnectionsException nce) {
            // This should never happen since it looks like the code doesn't actualy throw an exception
            log.warn("Attempting to adjust max-in-flight but found no connections.", nce);
        }
    }

    public void setMessagesPerBatch(int messagesPerBatch) {
        this.messagesPerBatch = messagesPerBatch;
    }

    public void setLookupPeriod(long periodMillis) {
        this.lookupPeriod = periodMillis;
    }

    /**
     * for internal use.  called when a connection is disconnected
     * @param connection
     */
    public synchronized void _disconnected(Connection connection) {
        log.warn("Disconnected!" + connection);
        this.connections.remove(connection);
    }

    public void close() {
        this.timer.cancel();
        this.connections.close();
        this.bootstrap.releaseExternalResources();
    this.executor.shutdown();
    }
}
