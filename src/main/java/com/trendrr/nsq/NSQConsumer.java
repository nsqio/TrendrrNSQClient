package com.trendrr.nsq;

import com.google.common.base.Throwables;
import com.trendrr.nsq.exceptions.DisconnectedException;
import com.trendrr.nsq.frames.ErrorFrame;
import com.trendrr.nsq.frames.NSQFrame;
import com.trendrr.nsq.lookup.NSQLookup;
import org.apache.logging.log4j.LogManager;
import sun.plugin.dom.exception.InvalidStateException;

import java.io.IOException;
import java.util.List;

public class NSQConsumer extends AbstractNSQClient {

	private NSQLookup lookup;
	private String topic = null;
	private String channel = null;
	private NSQMessageCallback callback;
    private Connection connection;
	
    
	public NSQConsumer(NSQLookup lookup, String topic, String channel, NSQMessageCallback callback) {
		this.lookup = lookup;
		this.topic = topic;
		this.channel = channel;
		this.callback = callback;
	}

    @Override
	protected Connection createConnection(String address, int port) {
        connection = super.createConnection(address, port);

        connection.setCallback(callback);

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
        } catch (DisconnectedException e) {
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
