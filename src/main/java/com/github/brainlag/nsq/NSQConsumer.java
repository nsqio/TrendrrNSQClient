package com.github.brainlag.nsq;

import com.github.brainlag.nsq.callbacks.NSQErrorCallback;
import com.github.brainlag.nsq.callbacks.NSQMessageCallback;
import com.github.brainlag.nsq.exceptions.NoConnectionsException;
import com.github.brainlag.nsq.frames.ErrorFrame;
import com.github.brainlag.nsq.frames.NSQFrame;
import com.github.brainlag.nsq.lookup.NSQLookup;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import io.netty.bootstrap.Bootstrap;
import org.apache.logging.log4j.LogManager;

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
    private NSQConfig config;

    private Timer timer;
    private ExecutorService executor = Executors.newCachedThreadPool();
    private Bootstrap bootstrap;
    private Map<ServerAddress, Connection> connections = Maps.newHashMap();

    private boolean started = false;

    private int messagesPerBatch = 200;
    private long lookupPeriod = 60 * 1000; // how often to recheck for new nodes (and clean up non responsive nodes)

    public NSQConsumer(NSQLookup lookup, String topic, String channel, NSQMessageCallback callback) {
        this(lookup, topic, channel, callback, new NSQConfig());
    }

    public NSQConsumer(NSQLookup lookup, String topic, String channel, NSQMessageCallback callback,
                       NSQConfig config) {
        this(lookup, topic, channel, callback, config, null);
    }

    public NSQConsumer(NSQLookup lookup, String topic, String channel, NSQMessageCallback callback,
                       NSQConfig config, NSQErrorCallback errCallback) {
        this.lookup = lookup;
		this.topic = topic;
		this.channel = channel;
        this.config = config;
        this.callback = callback;
        this.errorCallback = errCallback;
    }

    public NSQConsumer start() {
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
        return this;
    }

    private Connection createConnection(ServerAddress serverAddress) {
        try {
            Connection connection = new Connection(serverAddress, config, executor);

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
                        throw new IllegalStateException(err);
                    }
                }
            }
        } catch (TimeoutException e) {
            LogManager.getLogger(this).warn("No clean disconnect", e);
        }
    }

    public NSQConsumer setMessagesPerBatch(int messagesPerBatch) {
        if (!started) {
            this.messagesPerBatch = messagesPerBatch;
        }
        return this;
    }

    public NSQConsumer setLookupPeriod(long periodMillis) {
        if (!started) {
            this.lookupPeriod = periodMillis;
        }
        return this;
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
    public NSQConsumer setExecutor(ExecutorService executor) {
        if (!started) {
            this.executor = executor;
        }
        return this;
    }

    private Set<ServerAddress> lookupAddresses() {
        try {
            return lookup.lookup(topic);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
