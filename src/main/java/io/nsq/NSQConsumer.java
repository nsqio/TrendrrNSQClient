package io.nsq;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import io.netty.bootstrap.Bootstrap;
import io.nsq.callbacks.NSQErrorCallback;
import io.nsq.callbacks.NSQMessageCallback;
import io.nsq.exceptions.NoConnectionsException;
import io.nsq.frames.ErrorFrame;
import io.nsq.frames.NSQFrame;
import io.nsq.lookup.NSQLookup;
import org.apache.logging.log4j.LogManager;
import sun.plugin.dom.exception.InvalidStateException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class NSQConsumer {

	private NSQLookup lookup;
	private String topic = null;
	private String channel = null;
	private NSQMessageCallback callback;
    private NSQErrorCallback errorCallback;

    private Timer timer;
    private ExecutorService executor = Executors.newCachedThreadPool();
    private Bootstrap bootstrap;
    private Map<ServerAddress, Connection> connections = Maps.newHashMap();

    private boolean started = false;

    private int messagesPerBatch = 200;
    private long lookupPeriod = 60 * 1000; // how often to recheck for new nodes (and clean up non responsive nodes)

    public NSQConsumer(NSQLookup lookup, String topic, String channel, NSQMessageCallback callback) {
        this(lookup, topic, channel, callback, null);
    }

    public NSQConsumer(NSQLookup lookup, String topic, String channel, NSQMessageCallback callback,
                       NSQErrorCallback errCallback) {
        this.lookup = lookup;
		this.topic = topic;
		this.channel = channel;
		this.callback = callback;
        this.errorCallback = errCallback;
    }

    public void start() {
        if (!started) {
            started = true;
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

    private Connection createConnection(ServerAddress serverAddress) {
        try {
            Connection connection = new Connection(serverAddress, executor);

            connection.setMessageCallback(callback);
            connection.setErrorCallback(errorCallback);
            connection.command(NSQCommand.instance("SUB " + topic + " " + this.channel));
            connection.command(NSQCommand.instance("RDY " + connection.getMessagesPerBatch()));
            connection.setMessagesPerBatch(messagesPerBatch);

            return connection;
        } catch (NoConnectionsException e) {
            return null;
        }
	}

    public void shutdown() {
        this.timer.cancel();
        cleanClose();
    }

    private void cleanClose() {
        NSQCommand command = NSQCommand.instance("CLS");
        try {
            for (Connection connection : connections.values()) {
                NSQFrame frame = connection.commandAndWait(command);
                if (frame instanceof ErrorFrame) {
                    String err = ((ErrorFrame) frame).getErrorMessage();
                    if (err.startsWith("E_INVALID")) {
                        throw new InvalidStateException(err);
                    }
                }
            }
        } catch (TimeoutException e) {
            LogManager.getLogger(this).warn("No clean disconnect", e);
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


    private void connect() {
        Set<ServerAddress> newAddresses = lookupAddresses();
        Set<ServerAddress> oldAddresses = connections.keySet();
        oldAddresses.removeAll(newAddresses);
        for (ServerAddress server : oldAddresses) {
            connections.get(server).close();
            connections.remove(server);
        }
        for (Iterator<Map.Entry<ServerAddress, Connection>> it = connections.entrySet().iterator(); it.hasNext(); ) {
            if (!it.next().getValue().isConnected()) {
                it.remove();
            }
        }
        for (ServerAddress server : newAddresses) {
            if (!connections.containsKey(server)) {
                connections.put(server, createConnection(server));
            }
        }
    }

    /**
     * This is the executor where the callbacks happen.
     * The executer can only changed before the client is started.
     * Default is a cached threadpool.
     *
     * @param executor
     */
    public void setExecutor(ExecutorService executor) {
        if (!started) {
            this.executor = executor;
        }
    }

    private Set<ServerAddress> lookupAddresses() {
        try {
            return lookup.lookup(topic);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
