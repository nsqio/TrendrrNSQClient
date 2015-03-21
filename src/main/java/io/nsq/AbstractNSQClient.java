package io.nsq;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.nsq.exceptions.NoConnectionsException;
import io.nsq.netty.NSQClientInitializer;
import org.apache.logging.log4j.LogManager;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;


/**
 * Base class for producer and consumer
 */
public abstract class AbstractNSQClient {
	public static byte[] MAGIC_PROTOCOL_VERSION = "  V2".getBytes();

	private int messagesPerBatch = 200;
	private long lookupPeriod = 60 * 1000; // how often to recheck for new nodes (and clean up non responsive nodes)

    protected Connections connections = new Connections();
    private Bootstrap bootstrap = null;
    private Timer timer = null;
    private Executor executor = Executors.newSingleThreadExecutor();

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
	 */
	public synchronized void setNettyExecutors() {
		bootstrap = new Bootstrap();
        bootstrap.group(new NioEventLoopGroup());
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new NSQClientInitializer());
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
		Channel channel = future.awaitUninterruptibly().channel();
		if (!future.isSuccess()) {
            LogManager.getLogger(this).error("Caught", future.cause());
		    return null;
		}
        LogManager.getLogger(this).info("Creating connection: " + address + " : " + port);
		Connection conn = new Connection(address, port, channel, this);
		conn.setMessagesPerBatch(this.messagesPerBatch);

        ByteBuf buf = Unpooled.buffer();
		buf.writeBytes(MAGIC_PROTOCOL_VERSION);
		channel.write(buf);
        channel.flush();

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
            LogManager.getLogger(this).error("Caught", e);
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
        if (bootstrap == null) {
            //create default bootstrap
            setNettyExecutors();
        }

        List<ConnectionAddress> addresses = lookupAddresses();


		for (ConnectionAddress addr : addresses ) {
            int num = addr.getPoolsize() - connections.connectionSize(addr.getHost(), addr.getPort());
            for (int i=0; i < num; i++) {
                Connection conn = createConnection(addr.getHost(), addr.getPort());
                connections.addConnection(conn);
            }
			//TODO: handle negative num? (i.e. if user lowered the poolsize we should kill some connections)
		}
        cleanupOldConnections();
    }

	/**
	 * will run through and remove any connections that have not recieved a ping in the last 2 minutes.
	 */
	public synchronized void cleanupOldConnections() {
		Date cutoff = new Date(new Date().getTime() - (1000*60*2));
		try {
			for (Connection c : this.connections.getConnections()) {
				if (cutoff.after(c.getLastHeartbeat())) {
                    LogManager.getLogger(this).warn("Removing dead connection: " + c.getHost() + ":" + c.getPort());
					c.close();
					connections.remove(c);
				}
			}
		} catch (NoConnectionsException e) {
			//ignore
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
        LogManager.getLogger(this).warn("Disconnected!" + connection);
		this.connections.remove(connection);
	}

	public void close() {
		this.timer.cancel();
		this.connections.close();
	}
}
