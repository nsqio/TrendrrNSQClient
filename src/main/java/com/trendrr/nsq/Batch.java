/**
 *
 */
package com.trendrr.nsq;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * A class that will collect messages and send as a batch after N messages or N bytes are written.
 *
 *
 *
 * @author Dustin Norlander
 * @created Jan 22, 2013
 *
 */
public class Batch {

    protected static Logger log = LoggerFactory.getLogger(Batch.class);

    protected long maxBytes = 1024768l - 50000;// 1024768 is the default nsqd message max.  subtract 50k so we dont accidentally go over the limit.
    protected int maxMessages = 500;
    protected long maxSeconds = 30;
    protected String topic;

    protected long totalMessages;
    protected long totalBytes;
    protected Date expire = null;
    protected List<byte[]> messages;
    protected BatchCallback callback = null;

    public Batch(String topic, BatchCallback callback) {
        this.topic = topic;
        this.callback = callback;
        this.getAndClear(); //set the default values for everything.
    }

    public synchronized BatchCallback getCallback() {
        return callback;
    }


    public synchronized void setCallback(BatchCallback callback) {
        this.callback = callback;
    }





    public synchronized void addMessage(byte[] bytes) {
        if (expire == null)
            expire = new Date(new Date().getTime() + (1000*this.maxSeconds));
        totalMessages++;
        totalBytes += bytes.length;
        totalBytes += 4; //for the message size
        messages.add(bytes);
    }



    /**
     * gets and clears if the current batch is ready.  else returns null
     * @return
     */
    public synchronized List<byte[]> getAndClearIfReady() {
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

    public synchronized List<byte[]> getAndClear() {
        this.totalBytes = 4;
        this.totalMessages = 0;
        this.expire = null;
        List<byte[]> messages = this.messages;
        this.messages = new ArrayList<byte[]>();
        return messages;

    }

    public synchronized long getMaxSeconds() {
        return maxSeconds;
    }

    public synchronized void setMaxSeconds(long maxSeconds) {
        this.maxSeconds = maxSeconds;
    }


    public synchronized long getMaxBytes() {
        return maxBytes;
    }

    public synchronized void setMaxBytes(long maxBytes) {
        this.maxBytes = maxBytes;
    }

    public synchronized int getMaxMessages() {
        return maxMessages;
    }

    public synchronized void setMaxMessages(int maxMessages) {
        this.maxMessages = maxMessages;
    }

    public synchronized String getTopic() {
        return topic;
    }

    public synchronized void setTopic(String topic) {
        this.topic = topic;
    }
}
