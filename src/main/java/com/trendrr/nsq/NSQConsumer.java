package com.trendrr.nsq;
/**
 *
 */

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * @author Dustin Norlander
 * @created Jan 14, 2013
 *
 */
public class NSQConsumer extends AbstractNSQClient {

    protected static Logger log = LoggerFactory.getLogger(NSQConsumer.class);

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
        return lookup.lookup(topic);
    }
}
