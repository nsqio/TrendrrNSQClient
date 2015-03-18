/**
 *
 */
package com.trendrr.nsq;

import java.io.UnsupportedEncodingException;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Dustin Norlander
 * @created Jan 15, 2013
 *
 */
public class NSQMessage {

    protected static Logger log = LoggerFactory.getLogger(NSQMessage.class);
    protected byte[] id;
    protected int attempts;
    protected Date timestamp;
    protected byte[] message;
    protected Connection connection;

    /**
     * Finished processing this message, let nsq know so it doesnt get reprocessed.
     */
    public void finished() {
        try {
            this.connection.command(NSQCommand.instance("FIN " + new String(id, "ascii")));
        } catch (UnsupportedEncodingException e) {
            log.error("Caught", e);
        }
    }

    /**
     * indicates a problem with processing, puts it back on the queue.
     */
    public void requeue(int timeoutMillis) {
        try {
            this.connection.command(NSQCommand.instance("REQ " + new String(id, "ascii") + " " + timeoutMillis));
        } catch (UnsupportedEncodingException e) {
            log.error("Caught", e);
        }
    }

    public void requeue() {
        requeue(0);
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public byte[] getId() {
        return id;
    }

    public void setId(byte[] id) {
        this.id = id;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public byte[] getMessage() {
        return message;
    }

    public void setMessage(byte[] message) {
        this.message = message;
    }
}
