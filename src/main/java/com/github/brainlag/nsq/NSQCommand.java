package com.github.brainlag.nsq;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class NSQCommand {
    private String line;
    private List<byte[]> data = new ArrayList<>();

    private NSQCommand() { /** no instances */}

    public void addBytes(byte[] bytes) {
        data.add(bytes);
    }

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        if (!line.endsWith("\n")) {
            line = line + "\n";
        }

        this.line = line;
    }

    public List<byte[]> getData() {
        return data;
    }

    public void setData(List<byte[]> data) {
        this.data = data;
    }

    public String toString() {
        return this.getLine().trim();
    }

    // ASCII stores a reference to the charset needed for commands
    public static final Charset ASCII = Charset.forName("ascii");

    // Identify creates a new Command to provide information about the client.  After connecting,
    // it is generally the first message sent.
    //
    // The supplied body should be a map marshaled into JSON to provide some flexibility
    // for this command to evolve over time.
    //
    // See http://nsq.io/clients/tcp_protocol_spec.html#identify for information
    // on the supported options
    public static NSQCommand identify(byte[] body) {
        return NSQCommand.instance("IDENTIFY", body);
    }

    // Touch creates a new Command to reset the timeout for
    // a given message (by id)
    public static NSQCommand touch(byte[] messageID) {
        return NSQCommand.instance("TOUCH " + new String(messageID, ASCII));
    }

    // Finish creates a new Command to indiciate that
    // a given message (by id) has been processed successfully
    public static NSQCommand finish(byte[] messageID) {
        return NSQCommand.instance("FIN " + new String(messageID, ASCII));
    }

    // Subscribe creates a new Command to subscribe to the given topic/channel
    public static NSQCommand subscribe(String topic, String channel) {
        return NSQCommand.instance("SUB " + topic + " " + channel);
    }

    // StartClose creates a new Command to indicate that the
    // client would like to start a close cycle.  nsqd will no longer
    // send messages to a client in this state and the client is expected
    // finish pending messages and close the connection
    public static NSQCommand startClose() {
        return NSQCommand.instance("CLS");
    }

    public static NSQCommand requeue(byte[] messageID, int timeoutMillis) {
        return NSQCommand.instance("REQ " + new String(messageID, ASCII) + " " + timeoutMillis);
    }

    // Nop creates a new Command that has no effect server side.
    // Commonly used to respond to heartbeats
    public static NSQCommand nop() {
        return NSQCommand.instance("NOP");
    }

    // Ready creates a new Command to specify
    // the number of messages a client is willing to receive
    public static NSQCommand ready(int rdy) {
        return NSQCommand.instance("RDY " + rdy);
    }

    // Publish creates a new Command to write a message to a given topic
    public static NSQCommand publish(String topic, byte[] message) {
        return NSQCommand.instance("PUB " + topic, message);
    }

    // MultiPublish creates a new Command to write more than one message to a given topic
    // (useful for high-throughput situations to avoid roundtrips and saturate the pipe)
    // Note: can only be used with more than 1 bodies!
    public static NSQCommand multiPublish(String topic, List<byte[]> bodies) {
        NSQCommand cmd = NSQCommand.instance("MPUB " + topic);
        cmd.setData(bodies);
        return cmd;
    }

    private static NSQCommand instance(String line) {
        NSQCommand n = new NSQCommand();
        n.setLine(line);
        return n;
    }

    private static NSQCommand instance(String line, byte[] bytes) {
        NSQCommand n = instance(line);
        n.addBytes(bytes);
        return n;
    }

}
