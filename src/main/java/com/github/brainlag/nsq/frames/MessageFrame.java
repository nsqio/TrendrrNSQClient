package com.github.brainlag.nsq.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class MessageFrame extends NSQFrame {
    private long timestamp;
    private int attempts;
    private byte[] messageId = new byte[16];
    private byte[] messageBody;

    @Override
    public void setData(byte[] bytes) {
        //parse the bytes
        super.setData(bytes);

        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        timestamp = buf.readLong();
        attempts = buf.readShort();
        buf.readBytes(messageId);
        ByteBuf messageBodyBuf = buf.readBytes(buf.readableBytes());
        if (messageBodyBuf.hasArray()) {
            messageBody = messageBodyBuf.array();
        } else {
            byte[] array = new byte[messageBodyBuf.readableBytes()];
            messageBodyBuf.readBytes(array);
            messageBody = array;
        }
        buf.release();
        messageBodyBuf.release();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getAttempts() {
        return attempts;
    }

    public byte[] getMessageId() {
        return messageId;
    }

    public byte[] getMessageBody() {
        return messageBody;
    }

}
