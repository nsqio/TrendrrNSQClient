package io.nsq;

public class ServerAddress {

    public ServerAddress(final String host, final int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String toString() {
        return host + ":" + port;
    }

    private String host;
    private int port;
}
