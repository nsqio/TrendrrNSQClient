/**
 * 
 */
package com.trendrr.nsq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Holds the details of a connection
 * @author Dustin Norlander
 * @created Jan 22, 2013
 * 
 */
public class ConnectionAddress {

	private static Logger log = LoggerFactory.getLogger(ConnectionAddress.class);

    private static final int DEFAULT_POOL_SIZE = 1;

    private final String host;
    private final int port;
	private final int poolsize;

    public ConnectionAddress(String host, int port) {
        this(host, port, DEFAULT_POOL_SIZE);
    }

    public ConnectionAddress(String host, int port, int poolsize) {
        this.host = host;
        this.port = port;
        this.poolsize = poolsize;
    }

    /**
	 * How many connections should we have in place?
	 * @return
	 */
	public int getPoolsize() {
		return poolsize;
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

        ConnectionAddress that = (ConnectionAddress) o;

        if (port != that.port) return false;
        if (!host.equals(that.host)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + port;
        return result;
    }
}
