package io.nsq;

import com.google.common.base.Throwables;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.AttributeKey;
import io.nsq.callbacks.NSQErrorCallback;
import io.nsq.callbacks.NSQMessageCallback;
import io.nsq.frames.ErrorFrame;
import io.nsq.frames.MessageFrame;
import io.nsq.frames.NSQFrame;
import io.nsq.frames.ResponseFrame;
import org.apache.logging.log4j.LogManager;

import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public class Connection {
    public static final AttributeKey<Connection> STATE =
            AttributeKey.valueOf("Connection.state");
    private Channel channel;
    private int heartbeats = 0;
    private Date lastHeartbeat = new Date();
    private NSQMessageCallback callback = null;
    private AtomicLong totalMessages = new AtomicLong(0l);
    private int messagesPerBatch = 200;
    private AbstractNSQClient client = null;
    private String host = null;
    private int port;
    private LinkedBlockingQueue<NSQCommand> requests = new LinkedBlockingQueue<>(1);
    private LinkedBlockingQueue<NSQFrame> responses = new LinkedBlockingQueue<>(1);


    public Connection(String host, int port, Channel channel, AbstractNSQClient client) {
        this.channel = channel;
        this.channel.attr(STATE).set(this);
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
                        LogManager.getLogger(this).error("Incoming frame error", e);
                        //TODO: what to do here? we should probably disconnect!
                        this.close();
                    }
                }
                return;
            }
        }

        if (frame instanceof ErrorFrame) {
            this.responses.add(frame);
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
                LogManager.getLogger(this).warn("NO CAllback, dropping message: " + message);
            } else {
                this.callback.message(message);
            }
            return;
        }

        LogManager.getLogger(this).warn("Unknown frame type: " + frame);
    }


    void heartbeat() {
        LogManager.getLogger(this).info("HEARTBEAT!");
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

    protected synchronized void setLastHeartbeat() {
        this.lastHeartbeat = new Date();
    }

    public synchronized Date getLastHeartbeat() {
        return lastHeartbeat;
    }

    public long getTotalMessages() {
        return totalMessages.get();
    }

    public NSQMessageCallback getMessageCallback() {
        return callback;
    }


    public void setMessageCallback(NSQMessageCallback callback) {
        this.callback = callback;
    }

    public void setErrorCallback(NSQErrorCallback callback) {

    }

    public void close() {
        try {
            channel.close().await(10000);
        } catch (Exception x) {
            LogManager.getLogger(this).error("Caught", x);
        }
        LogManager.getLogger(this).warn("Close called on connection: " + this);
        this._disconnected();
    }

    public NSQFrame commandAndWait(NSQCommand command) throws TimeoutException {
        try {
            if (!this.requests.offer(command, 15, TimeUnit.SECONDS)) {
                throw new TimeoutException("command: " + command + " timedout");
            }

            this.responses.clear(); //clear the response queue if needed.
            ChannelFuture fut = this.command(command);

            if (!fut.await(15, TimeUnit.SECONDS)) {
                throw new TimeoutException("command: " + command + " timedout");
            }

            NSQFrame frame = this.responses.poll(15, TimeUnit.SECONDS);
            if (frame == null) {
                throw new TimeoutException("command: " + command + " timedout");
            }

            this.requests.poll(); //clear the request object
            return frame;

        } catch (TimeoutException x) {
            this.close();
            throw x;
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }

    }

    public ChannelFuture command(NSQCommand command) {
        return this.channel.writeAndFlush(command);
    }

    public String toString() {
        return "NSQCONNECTION : " + this.host + " : " + this.port;
    }
}
