package io.nsq;

import com.google.common.base.Throwables;
import io.nsq.callbacks.NSQErrorCallback;
import io.nsq.callbacks.NSQMessageCallback;
import io.nsq.frames.ErrorFrame;
import io.nsq.frames.NSQFrame;
import io.nsq.lookup.NSQLookup;
import org.apache.logging.log4j.LogManager;
import sun.plugin.dom.exception.InvalidStateException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class NSQConsumer extends AbstractNSQClient {

	private NSQLookup lookup;
	private String topic = null;
	private String channel = null;
	private NSQMessageCallback callback;
    private NSQErrorCallback errorCallback;
    private Connection connection;

    public NSQConsumer(NSQLookup lookup, String topic, String channel, NSQMessageCallback callback) {
        this(lookup, topic, channel, callback, null);
    }

    public NSQConsumer(NSQLookup lookup, String topic, String channel, NSQMessageCallback callback, NSQErrorCallback errCallback) {
        this.lookup = lookup;
		this.topic = topic;
		this.channel = channel;
		this.callback = callback;
        this.errorCallback = errCallback;
    }

    @Override
	protected Connection createConnection(String address, int port) {
        connection = super.createConnection(address, port);

        connection.setMessageCallback(callback);
        connection.setErrorCallback(errorCallback);

        connection.command(NSQCommand.instance("SUB " + topic + " " + this.channel));
        connection.command(NSQCommand.instance("RDY " + connection.getMessagesPerBatch()));
		return connection;
	}

    @Override
    public void close() {
        cleanClose();
        super.close();
    }

    private void cleanClose() {
        NSQCommand command = NSQCommand.instance("CLS");
        try {
            NSQFrame frame = connection.commandAndWait(command);
            if (frame instanceof ErrorFrame) {
                String err = ((ErrorFrame) frame).getErrorMessage();
                if (err.startsWith("E_INVALID")) {
                    throw new InvalidStateException(err);
                }
            }
        } catch (TimeoutException e) {
            LogManager.getLogger().warn("No clean disconnect", e);
        }
    }

	@Override
	public List<ConnectionAddress> lookupAddresses() {
        try {
            return lookup.lookup(topic);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
