package io.nsq;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.nsq.netty.NSQClientInitializer;
import org.apache.logging.log4j.LogManager;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;


/**
 * Base class for producer and consumer
 */
public abstract class AbstractNSQClient {
	public static byte[] MAGIC_PROTOCOL_VERSION = "  V2".getBytes();

	private int messagesPerBatch = 200;
	private long lookupPeriod = 60 * 1000; // how often to recheck for new nodes (and clean up non responsive nodes)

	protected Map<ServerAddress, Connection> connections = new ConcurrentHashMap<>();
	private Bootstrap bootstrap = null;
    private Timer timer = null;
	private Executor executor = Executors.newCachedThreadPool();

	private volatile boolean started = false;

	/**
	 * connects, ready to produce.
	 */
	public void start() {
		if (!started) {
			started = true;
			initNetty();
			//connect once otherwise we might have to wait one lookupPeriod
			connect();
			timer = new Timer();
			timer.schedule(new TimerTask() {
				@Override
				public void run() {
					connect();
				}
			}, lookupPeriod, lookupPeriod);
		}
	}

	/**
	 * Should return a list of all the addresses that we should be currently connected to.
	 * @return
	 */
	public abstract List<ServerAddress> lookupAddresses();

	/**
	 * This is the executor where the callbacks happen.
	 * The executer can only changed before the client is started.
	 * Default is a cached threadpool.
	 * @param executor
	 */
	public void setExecutor(Executor executor) {
		if (!started) {
			this.executor = executor;
		}
	}

	protected Executor getExecutor() {
		return executor;
	}

	/**
	 * use this if you want to specify your own netty executors. by default will use
	 *
	 * Executors.newCachedThreadPool()
	 */
	public void initNetty() {
		bootstrap = new Bootstrap();
        bootstrap.group(new NioEventLoopGroup());
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new NSQClientInitializer());
	}

	/**
	 * Creates a new connection object.
	 *
	 * Handles connection and sending magic protocol
	 * @param serverAddress of a nsqd server
	 * @return connection to a nsqd server
	 */
	protected Connection createConnection(ServerAddress serverAddress) {

		// Start the connection attempt.
		ChannelFuture future = bootstrap.connect(new InetSocketAddress(serverAddress.getHost(), serverAddress.getPort()));

		// Wait until the connection attempt succeeds or fails.
		Channel channel = future.awaitUninterruptibly().channel();
		if (!future.isSuccess()) {
            LogManager.getLogger(this).error("Caught", future.cause());
		    return null;
		}
		LogManager.getLogger(this).info("Creating connection: " + serverAddress.toString());
		Connection conn = new Connection(serverAddress, channel, this);
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
			LogManager.getLogger(this).error("Local host name could not resolved", e);
			conn.close();
			connections.remove(serverAddress);
			return null;
		}

		return conn;
	}

	protected synchronized void connect() {
		List<ServerAddress> addresses = lookupAddresses();

		for (ServerAddress addr : addresses) {
			if (!connections.containsKey(addr)) {
				Connection conn = createConnection(addr);
				if (conn != null) {
					connections.putIfAbsent(addr, conn);
				}
			}
		}
    }

	public void setMessagesPerBatch(int messagesPerBatch) {
		this.messagesPerBatch = messagesPerBatch;
	}

	public void setLookupPeriod(long periodMillis) {
		if (!started) {
			this.lookupPeriod = periodMillis;
		}
	}

	protected long getLookupPeriod() {
		return lookupPeriod;
	}

	public void close() {
		connections.values().forEach((con) -> con.close());
		connections.clear();
		this.timer.cancel();
	}
}
