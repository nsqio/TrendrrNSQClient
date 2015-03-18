/**
 *
 */
package com.trendrr.nsq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Dustin Norlander
 * @created Jan 22, 2013
 *
 */
public class ConnectionAddress {

    protected static Logger log = LoggerFactory.getLogger(ConnectionAddress.class);

    private int poolsize = 1;

    /**
     * How many connections should we have in place?
     * @return
     */
    public int getPoolsize() {
        return poolsize;
    }
    public void setPoolsize(int poolsize) {
        this.poolsize = poolsize;
    }


    public String getHost() {
        return host;
    }
    public void setHost(String host) {
        this.host = host;
    }
    public int getPort() {
        return port;
    }
    public void setPort(int port) {
        this.port = port;
    }
    private String host;
    private int port;
}
