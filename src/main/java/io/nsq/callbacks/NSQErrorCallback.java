package io.nsq.callbacks;

@FunctionalInterface
public interface NSQErrorCallback {

    public void error(Exception x);
}
