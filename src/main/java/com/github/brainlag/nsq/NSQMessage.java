package com.github.brainlag.nsq;

import java.util.Date;

public class NSQMessage {

    private byte[] id;
    private int attempts;
    private Date timestamp;
    private byte[] message;
    private Connection connection;

    /**
     * Finished processing this message, let nsq know so it doesnt get reprocessed.
     */
    public void finished() {
        connection.command(NSQCommand.finish(this.id));
    }

    public void touch() {
        connection.command(NSQCommand.touch(this.id));
    }

    /**
     * indicates a problem with processing, puts it back on the queue.
     */
    public void requeue(int timeoutMillis) {
        connection.command(NSQCommand.requeue(this.id, timeoutMillis));
    }

    public void requeue() {
        requeue(0);
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    protected Connection getConnection() {
        return connection;
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
