/**
 *
 */
package com.trendrr.nsq;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.trendrr.nsq.exceptions.BadMessageException;
import com.trendrr.nsq.exceptions.BadTopicException;
import com.trendrr.nsq.exceptions.DisconnectedException;
import com.trendrr.nsq.exceptions.NoConnectionsException;
import com.trendrr.nsq.frames.ErrorFrame;
import com.trendrr.nsq.frames.NSQFrame;
import com.trendrr.nsq.frames.ResponseFrame;


/**
 * @author Dustin Norlander
 * @created Jan 22, 2013
 *
 */
public class NSQProducer extends AbstractNSQClient {

    protected static Logger log = LoggerFactory.getLogger(NSQProducer.class);

    List<ConnectionAddress> addresses = new ArrayList<ConnectionAddress>();

    ConcurrentHashMap<String, Batch> batches = new ConcurrentHashMap<String, Batch>();

    /**
     * If no connections are available, will try this many times with 5 second pause between, before throwing a
     * no connections available exception.
     */
    int connectionRetries = 5;

    public void configureBatch(String topic, BatchCallback callback, Integer maxMessages, Long maxBytes, Integer maxSeconds) {
        Batch batch = new Batch(topic, callback);
        if (maxBytes != null) {
            batch.setMaxBytes(maxBytes);
        }
        if (maxMessages != null) {
            batch.setMaxMessages(maxMessages);
        }
        if (maxSeconds != null) {
            batch.setMaxSeconds(maxSeconds);
        }
        Batch old = this.batches.put(topic, batch);
        if (old != null)
            this.sendBatch(old, old.getAndClear());
    }

    /**
     * flushes all batches
     */
    public void flushBatches() {
        for (String k : batches.keySet()) {
            Batch b = batches.remove(k);
            if (b == null)
                continue;
            this.sendBatch(b, b.getAndClear());
        }
    }

    /**
     * sends the batch, sending result to callback
     * @param batch
     */
    protected void sendBatch(Batch batch, List<byte[]> messages) {
        if (messages.isEmpty())
            return;
        try {
            this.produceMulti(batch.getTopic(), messages);
            //success!
            batch.getCallback().batchSuccess(batch.getTopic(), messages.size());
        } catch (Exception x) {
            batch.getCallback().batchError(x, batch.getTopic(), messages);
        }
    }

    /**
     * produces a message in batch.  configure the batching options via
     * configureBatch method.
     * @param topic
     * @param message
     */
    public void produceBatch(String topic, byte[] message) {
        if (!this.batches.containsKey(topic)) {
            //add the default setup.
            Batch def = new Batch(topic, new BatchCallback() {
                @Override
                public void batchSuccess(String topic, int num) {
                    //do nothing
                }
                @Override
                public void batchError(Exception ex, String topic, List<byte[]> messages) {
                    log.error("Default batch callback for topic: " + topic, ex);
                }
            });

            this.batches.putIfAbsent(topic, def);
        }
        Batch batch = this.batches.get(topic);
        batch.addMessage(message);
        //send the batch if it is ready.
        List<byte[]> messages = batch.getAndClearIfReady();
        if (messages != null) {
            this.sendBatch(batch, messages);
        }
    }


    protected synchronized Connection getConn() throws NoConnectionsException {
        NoConnectionsException ex = new NoConnectionsException("no connections", null);
        for (int i=0; i < this.connectionRetries; i++) {
            try {
                return this.connections.next();
            } catch (NoConnectionsException x) {
                ex = x;
                try {
                    Thread.sleep(5*1000);
                } catch (InterruptedException e) {}
                //try to reconnect..
                log.warn("Attempting to reconnect");
                this.connect();
            }
        }
        log.warn("Could not get a new connection within " + (this.connectionRetries*5) + " seconds. giving up..");
        throw ex;
    }

    /**
     * produce multiple messages.
     * @param topic
     * @param message
     * @throws DisconnectedException
     * @throws BadTopicException
     * @throws BadMessageException
     * @throws NoConnectionsException
     */
    public void produceMulti(String topic, List<byte[]> message) throws DisconnectedException, BadTopicException, BadMessageException, NoConnectionsException{
        if (message == null || message.isEmpty()) {
            return;
        }

        if (message.size() == 1) {
            //encoding will be screwed up if we MPUB a
            this.produce(topic, message.get(0));
            return;
        }

        Connection c = this.getConn();

        NSQCommand command = NSQCommand.instance("MPUB " + topic);
        command.setData(message);


        NSQFrame frame = c.commandAndWait(command);
        if (frame instanceof ResponseFrame) {
            c._setLastHeartbeat(); //TODO: remove once producer server heartbeats in place.
            return;
        }
        if (frame instanceof ErrorFrame) {
            String err = ((ErrorFrame)frame).getErrorMessage();
            if (err.startsWith("E_BAD_TOPIC")) {
                throw new BadTopicException(err);
            }
            if (err.startsWith("E_BAD_MESSAGE")) {
                throw new BadMessageException(err);
            }
        }
        //disconnect
        c.close();
        throw new DisconnectedException("Error happened: " + frame, null);

    }

    /**
     * @param topic
     * @param message
     * @throws NoConnectionsException
     */
    public void produce(String topic, byte[] message) throws DisconnectedException, BadTopicException, BadMessageException, NoConnectionsException{
        Connection c = this.getConn();

        NSQCommand command = NSQCommand.instance("PUB " + topic, message);
        NSQFrame frame = c.commandAndWait(command);
        if (frame instanceof ResponseFrame) {
            c._setLastHeartbeat(); //TODO: remove once server heartbeats in place.
            return;
        }
        if (frame instanceof ErrorFrame) {
            String err = ((ErrorFrame)frame).getErrorMessage();
            if (err.startsWith("E_BAD_TOPIC")) {
                throw new BadTopicException(err);
            }
            if (err.startsWith("E_BAD_MESSAGE")) {
                throw new BadMessageException(err);
            }
        }
        //disconnect
        c.close();
        throw new DisconnectedException("Error happened: " + frame, null);

    }


    /**
     * Adds a new connection.
     * @param host
     * @param port
     * @param poolsize
     */
    public synchronized NSQProducer addAddress(String host, int port, int poolsize) {
        ConnectionAddress addr = new ConnectionAddress();
        addr.setHost(host);
        addr.setPoolsize(poolsize);
        addr.setPort(port);
        this.addresses.add(addr);
        return this;
    }

    @Override
    public void close() {
        this.flushBatches();
        super.close();
    }

    /* (non-Javadoc)
     * @see com.trendrr.nsq.AbstractNSQClient#lookupAddresses()
     */
    @Override
    public synchronized List<ConnectionAddress> lookupAddresses() {
        return this.addresses;
    }
}
