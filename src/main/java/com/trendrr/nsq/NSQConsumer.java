package com.trendrr.nsq;

import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.List;

public class NSQConsumer extends AbstractNSQClient {

	NSQLookup lookup;
	String topic = null;
	String channel = null;
	NSQMessageCallback callback;
	
    
	public NSQConsumer(NSQLookup lookup, String topic, String channel, NSQMessageCallback callback) {
		this.lookup = lookup;
		this.topic = topic;
		this.channel = channel;
		this.callback = callback;
	}
	
	@Override
	protected Connection createConnection(String address, int port) {
		Connection conn = super.createConnection(address, port);
		
		conn.setCallback(callback);
		/*
		 * subscribe
		 */
		conn.command(NSQCommand.instance("SUB " + topic + " " + this.channel));
		conn.command(NSQCommand.instance("RDY " + conn.getMessagesPerBatch()));
		return conn;
		
	}
	/* (non-Javadoc)
	 * @see com.trendrr.nsq.AbstractNSQClient#lookupAddresses()
	 */
	@Override
	public List<ConnectionAddress> lookupAddresses() {
        try {
            return lookup.lookup(topic);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
