package io.nsq;

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
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public class Connection {
    public static final AttributeKey<Connection> STATE =
            AttributeKey.valueOf("Connection.state");
    private AbstractNSQClient client;
    private ServerAddress address;
    private Channel channel;
    private NSQMessageCallback callback = null;
    private AtomicLong totalMessages = new AtomicLong(0l);
    private int messagesPerBatch = 200;
    private LinkedBlockingQueue<NSQCommand> requests = new LinkedBlockingQueue<>(1);
    private LinkedBlockingQueue<NSQFrame> responses = new LinkedBlockingQueue<>(1);


    public Connection(final ServerAddress address, final Channel channel, final AbstractNSQClient client) {
        this.channel = channel;
        this.channel.attr(STATE).set(this);
        this.client = client;
        this.address = address;
    }

    public Executor getExecutor() {
        return client.getExecutor();
    }

    public boolean isRequestInProgress() {
        return requests.size() > 0;
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
                heartbeat();
                return;
            } else {
                if (!requests.isEmpty()) {
                    try {
                        responses.offer(frame, 20, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        LogManager.getLogger(this).error("Incoming frame error", e);
                        close();
                    }
                }
                return;
            }
        }

        if (frame instanceof ErrorFrame) {
            responses.add(frame);
            return;
        }

        if (frame instanceof MessageFrame) {
            MessageFrame msg = (MessageFrame) frame;
            long tot = totalMessages.incrementAndGet();
            if (tot % messagesPerBatch > (messagesPerBatch / 2)) {
                //request some more!
                command(NSQCommand.instance("RDY " + messagesPerBatch));
            }

            NSQMessage message = new NSQMessage();
            message.setAttempts(msg.getAttempts());
            message.setConnection(this);
            message.setId(msg.getMessageId());
            message.setMessage(msg.getMessageBody());
            message.setTimestamp(new Date(TimeUnit.NANOSECONDS.toMillis(msg.getTimestamp())));
            if (callback == null) {
                LogManager.getLogger(this).warn("NO CAllback, dropping message: " + message);
            } else {
                callback.message(message);
            }
            return;
        }

        LogManager.getLogger(this).warn("Unknown frame type: " + frame);
    }


    void heartbeat() {
        LogManager.getLogger(this).info("HEARTBEAT!");
        //send NOP here.
        command(NSQCommand.instance("NOP"));
    }

    public long getTotalMessages() {
        return totalMessages.get();
    }

    public void setMessageCallback(NSQMessageCallback callback) {
        this.callback = callback;
    }

    public void setErrorCallback(NSQErrorCallback callback) {

    }

    public void close() {
        LogManager.getLogger(this).info("Closing  connection: " + this);
        channel.disconnect();
    }

    public NSQFrame commandAndWait(NSQCommand command) throws TimeoutException {
        try {
            if (!requests.offer(command, 15, TimeUnit.SECONDS)) {
                throw new TimeoutException("command: " + command + " timedout");
            }

            responses.clear(); //clear the response queue if needed.
            ChannelFuture fut = command(command);

            if (!fut.await(15, TimeUnit.SECONDS)) {
                throw new TimeoutException("command: " + command + " timedout");
            }

            NSQFrame frame = responses.poll(15, TimeUnit.SECONDS);
            if (frame == null) {
                throw new TimeoutException("command: " + command + " timedout");
            }

            requests.poll(); //clear the request object
            return frame;
        } catch (InterruptedException e) {
            close();
            LogManager.getLogger(this).warn("Thread was interruped!", e);
        }
        return null;
    }

    public ChannelFuture command(NSQCommand command) {
        return channel.writeAndFlush(command);
    }

    public void deregister() {
        client.connections.remove(address);
    }

}
