package com.github.brainlag.nsq.callbacks;

import com.github.brainlag.nsq.exceptions.NSQException;

@FunctionalInterface
public interface NSQErrorCallback {

    void error(NSQException x);
}
