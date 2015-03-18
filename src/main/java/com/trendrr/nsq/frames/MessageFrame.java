/**
 *
 */
package com.trendrr.nsq.frames;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Dustin Norlander
 * @created Jan 14, 2013
 *
 */
public class MessageFrame extends NSQFrame {

    protected static Logger log = LoggerFactory.getLogger(MessageFrame.class);

    long timestamp;
    int attempts;
    byte[] messageId;
    byte[] messageBody;

    public MessageFrame() {
        this.frameId = 2;
    }

    @Override
    public void setData(byte[] bytes) {
        //parse the bytes
        super.setData(bytes);

        ChannelBuffer buf = ChannelBuffers.wrappedBuffer(bytes);
        this.timestamp = buf.readLong();
        this.attempts = buf.readShort();
        this.messageId = new byte[16];

        buf.readBytes(this.messageId);
        this.messageBody = buf.readBytes(buf.readableBytes()).array();
    }

    public long getTimestamp() {
        return timestamp;
    }


    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }


    public int getAttempts() {
        return attempts;
    }


    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }


    public byte[] getMessageId() {
        return messageId;
    }


    public void setMessageId(byte[] messageId) {
        this.messageId = messageId;
    }


    public byte[] getMessageBody() {
        return messageBody;
    }


    public void setMessageBody(byte[] messageBody) {
        this.messageBody = messageBody;
    }






}
