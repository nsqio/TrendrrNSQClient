package com.github.brainlag.nsq;

import com.github.brainlag.nsq.callbacks.NSQErrorCallback;
import com.github.brainlag.nsq.callbacks.NSQMessageCallback;
import com.github.brainlag.nsq.exceptions.NSQException;
import com.github.brainlag.nsq.exceptions.NoConnectionsException;
import com.github.brainlag.nsq.frames.ErrorFrame;
import com.github.brainlag.nsq.frames.MessageFrame;
import com.github.brainlag.nsq.frames.NSQFrame;
import com.github.brainlag.nsq.frames.ResponseFrame;
import com.github.brainlag.nsq.netty.NSQClientInitializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.AttributeKey;
import org.apache.logging.log4j.LogManager;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Connection {
    public static byte[] MAGIC_PROTOCOL_VERSION = "  V2".getBytes();
    public static final AttributeKey<Connection> STATE =
            AttributeKey.valueOf("Connection.state");
    private ExecutorService executor;
    private ServerAddress address;
    private Channel channel;
    private NSQMessageCallback callback = null;
    private NSQErrorCallback errorCallback = null;
    private AtomicLong totalMessages = new AtomicLong(0l);
    private int messagesPerBatch = 200;
    private LinkedBlockingQueue<NSQCommand> requests = new LinkedBlockingQueue<>(1);
    private LinkedBlockingQueue<NSQFrame> responses = new LinkedBlockingQueue<>(1);
    private static EventLoopGroup group = new NioEventLoopGroup();
    private NSQConfig config;


    public Connection(final ServerAddress serverAddress, NSQConfig config, ExecutorService executor) throws NoConnectionsException {
        this.address = serverAddress;
        this.executor = executor;
        this.config = config;
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new NSQClientInitializer());
        // Start the connection attempt.
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(serverAddress.getHost(),
                serverAddress.getPort()));

        // Wait until the connection attempt succeeds or fails.
        channel = future.awaitUninterruptibly().channel();
        if (!future.isSuccess()) {
            throw new NoConnectionsException("Could not connect to server", future.cause());
        }
        LogManager.getLogger(this).info("Created connection: " + serverAddress.toString());
        this.channel.attr(STATE).set(this);
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(MAGIC_PROTOCOL_VERSION);
        channel.write(buf);
        channel.flush();

        //indentify
        NSQCommand ident = NSQCommand.instance("IDENTIFY", config.toString().getBytes());
        try {
            NSQFrame response = commandAndWait(ident);
            LogManager.getLogger(this).info("Server identification: " + ((ResponseFrame) response).getMessage());
        } catch (TimeoutException e) {
            LogManager.getLogger(this).error("Creating connection timed out", e);
            close();
        }
    }

    public boolean isConnected() {
        return channel.isActive();
    }

    public Executor getExecutor() {
        return executor;
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
                        LogManager.getLogger(this).error("Thread was interruped, probably shuthing down", e);
                        close();
                    }
                }
                return;
            }
        }

        if (frame instanceof ErrorFrame) {
            if (errorCallback != null) {
                errorCallback.error(NSQException.of((ErrorFrame) frame));
            }
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
                LogManager.getLogger(this).warn("NO Callback, dropping message: " + message);
            } else {
                callback.message(message);
            }
            return;
        }

        LogManager.getLogger(this).warn("Unknown frame type: " + frame);
    }


    void heartbeat() {
        LogManager.getLogger(this).info("HEARTBEAT!");
        command(NSQCommand.instance("NOP"));
    }

    public long getTotalMessages() {
        return totalMessages.get();
    }

    public void setMessageCallback(NSQMessageCallback callback) {
        this.callback = callback;
    }

    public void setErrorCallback(NSQErrorCallback callback) {
        errorCallback = callback;
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

    public ServerAddress getServerAddress() {
        return address;
    }

    public NSQConfig getConfig() {
        return config;
    }
}
