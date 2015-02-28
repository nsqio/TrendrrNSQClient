package com.trendrr.nsq;

public interface NSQMessageCallback {

	public void message(NSQMessage message);

	public void error(Exception x);
}
