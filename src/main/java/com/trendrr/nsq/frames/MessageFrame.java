package com.trendrr.nsq.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class MessageFrame extends NSQFrame {

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

        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
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
