package com.trendrr.nsq.model;

import com.google.common.base.Preconditions;

/**
 * Simple container a host and port.
 */
public class HostAndPort {

    private String host;

    private int port;

    public HostAndPort(String host, int port) {
        Preconditions.checkArgument(host != null && host.length() > 0, "Supplied host is invalid [host=" + host + "]");
        Preconditions.checkArgument(port > 0, "Supplied port is invalid [port=" + port + "]");
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HostAndPort that = (HostAndPort) o;

        if (port != that.port) return false;
        if (host != null ? !host.equals(that.host) : that.host != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = host != null ? host.hashCode() : 0;
        result = 31 * result + port;
        return result;
    }

    @Override
    public String toString() {
        return "HostAndPort{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
