package io.nsq;

import io.nsq.callbacks.BatchErrorCallback;
import io.nsq.callbacks.BatchSuccessCallback;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * A class that will collect messages and send as a batch after N messages or N bytes are written.
 */
public class Batch {
    protected long maxBytes = 1024768l - 50000;// 1024768 is the default nsqd message max.  subtract 50k so we dont accidentally go over the limit.
    protected int maxMessages = 500;
    protected long maxSeconds = 30;
    protected String topic;

    protected long totalMessages;
    protected long totalBytes;
    protected Date expire = null;
    protected List<byte[]> messages;
    protected BatchSuccessCallback successCallback = null;
    protected BatchErrorCallback errorCallback = null;

    public Batch(String topic) {
        this.topic = topic;
        this.getAndClear(); //set the default values for everything.
    }

    public void setErrorCallback(BatchErrorCallback callback) {
        this.errorCallback = callback;
    }


    public void setSuccessCallback(BatchSuccessCallback callback) {
        this.successCallback = callback;
    }

    public void addMessage(byte[] bytes) {
        if (expire == null)
            expire = new Date(new Date().getTime() + (1000 * this.maxSeconds));
        totalMessages++;
        totalBytes += bytes.length;
        totalBytes += 4; //for the message size
        messages.add(bytes);
    }

    /**
     * gets and clears if the current batch is ready.  else returns null
     *
     * @return
     */
    public List<byte[]> getAndClearIfReady() {
        if (this.totalBytes >= this.maxBytes) {
            return getAndClear();
        }
        if (this.totalMessages >= this.maxMessages) {
            return getAndClear();
        }
        if (this.expire != null && this.expire.before(new Date())) {
            return getAndClear();
        }
        return null;
    }

    public List<byte[]> getAndClear() {
        this.totalBytes = 4;
        this.totalMessages = 0;
        this.expire = null;
        List<byte[]> messages = this.messages;
        this.messages = new ArrayList<byte[]>();
        return messages;

    }

    public long getMaxSeconds() {
        return maxSeconds;
    }

    public void setMaxSeconds(long maxSeconds) {
        this.maxSeconds = maxSeconds;
    }


    public long getMaxBytes() {
        return maxBytes;
    }

    public void setMaxBytes(long maxBytes) {
        this.maxBytes = maxBytes;
    }

    public int getMaxMessages() {
        return maxMessages;
    }

    public void setMaxMessages(int maxMessages) {
        this.maxMessages = maxMessages;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
