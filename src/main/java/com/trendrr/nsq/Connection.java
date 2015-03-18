package com.trendrr.nsq;

/**
 *
 */

import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.trendrr.nsq.exceptions.DisconnectedException;
import com.trendrr.nsq.frames.ErrorFrame;
import com.trendrr.nsq.frames.MessageFrame;
import com.trendrr.nsq.frames.NSQFrame;
import com.trendrr.nsq.frames.ResponseFrame;




/**
 * @author Dustin Norlander
 * @created Jan 14, 2013
 *
 */
public class Connection {

    protected static Logger log = LoggerFactory.getLogger(Connection.class);

    Channel channel;
    int heartbeats = 0;
    Date lastHeartbeat = new Date();

    NSQMessageCallback callback = null;
    AtomicLong totalMessages = new AtomicLong(0l);
    int messagesPerBatch = 200;

    AbstractNSQClient client = null;

    String host = null;
    int port;

    LinkedBlockingQueue<NSQCommand> requests = new LinkedBlockingQueue<NSQCommand>(1);
    LinkedBlockingQueue<NSQFrame> responses = new LinkedBlockingQueue<NSQFrame>(1);


    public Connection(String host, int port, Channel channel, AbstractNSQClient client) {
        this.channel = channel;
        this.channel.setAttachment(this);
        this.client = client;
        this.host = host;
        this.port = port;
    }

    /**
     * gets the owner of this connection (either a NSQProducer or NSQConsumer)
     */
    public AbstractNSQClient getParent() {
        return this.client;
    }

    public boolean isRequestInProgress() {
        return this.requests.size() > 0;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getMessagesPerBatch() {
        return messagesPerBatch;
    }

    public void setMessagesPerBatch(int messagesPerBatch) {
        this.messagesPerBatch = messagesPerBatch;
    }

    public void incoming(NSQFrame frame) {
        if (frame instanceof ResponseFrame) {
            if ("_heartbeat_".equals(((ResponseFrame) frame).getMessage())) {
                this.heartbeat();
                return;
            } else {
                if (!this.requests.isEmpty()) {
                    try {
                        this.responses.offer(frame, 20, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        log.error("Incoming frame error", e);
                        //TODO: what to do here? we should probably disconnect!
                        this.close();
                    }
                }
                return;
            }
        }

        if (frame instanceof ErrorFrame) {
            try {
                this.responses.offer(frame, 1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("Dropping incoming frame error", e);
            }
            return;
        }

        if (frame instanceof MessageFrame) {
            MessageFrame msg = (MessageFrame) frame;
            long tot = this.totalMessages.incrementAndGet();
            if (tot % messagesPerBatch > (messagesPerBatch / 2)) {
                //request some more!
                this.command(NSQCommand.instance("RDY " + this.messagesPerBatch));
            }

            NSQMessage message = new NSQMessage();
            message.setAttempts(msg.getAttempts());
            message.setConnection(this);
            message.setId(msg.getMessageId());
            message.setMessage(msg.getMessageBody());
            message.setTimestamp(new Date(TimeUnit.NANOSECONDS.toMillis(msg.getTimestamp())));
            if (this.callback == null) {
                log.warn("NO CAllback, dropping message: " + message);
            } else {
                this.callback.message(message);
            }
            return;
        }

        log.warn("Unknown frame type: " + frame);
    }


    void heartbeat() {
        log.info("HEARTBEAT!");
        this.heartbeats++;
        this.lastHeartbeat = new Date();
        //send NOP here.
        this.command(NSQCommand.instance("NOP"));
    }

    /**
     * called when this connection is disconnected socket level
     * this is used internally, generally close() should be used instead.
     */
    public void _disconnected() {
        //clean up anything that needs cleaning up.
        this.client._disconnected(this);
    }

    public int getHeartbeats() {
        return heartbeats;
    }

    /**
     * Do not use this, only here until server implements producer heartbeats.
     */
    public synchronized void _setLastHeartbeat() {
        this.lastHeartbeat = new Date();
    }

    public synchronized Date getLastHeartbeat() {
        return lastHeartbeat;
    }

    public long getTotalMessages() {
        return totalMessages.get();
    }

    public NSQMessageCallback getCallback() {
        return callback;
    }


    public void setCallback(NSQMessageCallback callback) {
        this.callback = callback;
    }


    public void close() {
        try {
            channel.close().await(10000);
        } catch (Exception x) {
            log.error("Caught", x);
        }
        log.warn("Close called on connection: " + this);
    }

    /**
     * issues a command and waits for the result
     *
     * @param command
     * @return
     * @throws Exception
     */
    public NSQFrame commandAndWait(NSQCommand command) throws DisconnectedException {

        try {
            try {

                if (!this.requests.offer(command, 15, TimeUnit.SECONDS)) {
                    //throw timeout, and disconnect?
                    throw new DisconnectedException("command: " + command + " timedout, disconnecting..", null);
                }

                this.responses.clear(); //clear the response queue if needed.
                ChannelFuture fut = this.command(command);

                if (!fut.await(15, TimeUnit.SECONDS)) {
                    //throw timeout, and disconnect?
                    throw new DisconnectedException("command: " + command + " timedout, disconnecting..", null);
                }

                NSQFrame frame = this.responses.poll(15, TimeUnit.SECONDS);
                if (frame == null) {
                    throw new DisconnectedException("command: " + command + " timedout, disconnecting..", null);
                }

                this.requests.poll(); //clear the request object
                return frame;

            } catch (DisconnectedException x) {
                throw x;
            } catch (Exception x) {
                throw new DisconnectedException("command: " + command + " timedout, disconnecting..", x);
            }
        } catch (DisconnectedException x) {
            //now disconnect this
            this.close();
            throw x;
        }
    }

    /**
     * issues a command.  doesnt wait on response, the future is only for delivery.
     *
     * @param command
     * @return
     */
    public ChannelFuture command(NSQCommand command) {
        return this.channel.write(command);
    }

    public String toString() {
        return "NSQCONNECTION : " + super.toString() + " " + this.host + " : " + this.port;
    }
}
