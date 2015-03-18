/**
 *
 */
package com.trendrr.nsq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.trendrr.nsq.exceptions.NoConnectionsException;


/**
 * Wraps a series of connections.
 *
 *
 * @author Dustin Norlander
 * @created Jan 22, 2013
 *
 */
public class Connections {

    protected static Logger log = LoggerFactory.getLogger(Connections.class);

    protected HashMap<String, List<Connection>> connections = new HashMap<String, List<Connection>>();
    protected List<Connection> connectionList = new ArrayList<Connection>();
    protected long rrCount = 0;

    public synchronized void addConnection(Connection connection) {
        if (connection == null) {
            return;
        }

        String key = connection.getHost() + ":" + connection.getPort();
        if (connectionList.contains(connection)) {
            log.warn("Already contains connection:" + connection);
            return;
        }

        if (connections.get(key) == null) {
            connections.put(key, new ArrayList<Connection>());
        }
        connections.get(key).add(connection);
        connectionList.add(connection);
    }

    public synchronized boolean containsConnection(String host, int port) {
        return connections.containsKey(this.getKey(host, port));
    }

    /**
     * returns the number of connections for a specified host + port
     * @param host
     * @param port
     * @return
     */
    public synchronized int connectionSize(String host, int port) {
        String key = this.getKey(host, port);
        List<Connection> c = this.connections.get(key);
        if (c == null)
            return 0;
        return c.size();
    }

    public synchronized int size() {
        return this.connectionList.size();
    }
    /**
     * returns a list of all current connections.
     *
     * @return
     */
    public synchronized List<Connection> getConnections() throws NoConnectionsException {
        //return a copy of the internal list so we dont have any
        //concurrent modification problems.
        return new ArrayList<Connection>(this.connectionList);
    }
    /**
     * round robin returns the next connection
     * @return
     */
    public synchronized Connection next() throws NoConnectionsException {
        if (connectionList.size() == 0) {
            log.warn("No connections available!");
            throw new NoConnectionsException("No connections available", null);
        }

        return connectionList.get((int)(rrCount++ % connectionList.size()));
    }

    public synchronized void remove(Connection connection) {
        String key = connection.getHost() + ":" + connection.getPort();
        if (!connectionList.contains(connection)) {
            log.warn("Does not contain : " + key + ": " + connection);
            return;
        }
        List<Connection> conn = connections.get(key);
        conn.remove(connection);
        if (conn.isEmpty()) {
            connections.remove(key);
        }
        this.connectionList.remove(connection);
        log.warn("REMOVED: " + key);
    }

    public synchronized void remove(String host, int port) {
        List<Connection> conns = this.connections.remove(host + ":" + port);
        if (conns == null) {
            log.warn("no connection : " +host + ":" + port);
            return;
        }
        this.connectionList.removeAll(conns);
    }

    public synchronized void close() {
        try {
            for (Connection c: this.getConnections()) {
                c.close();
            }
        } catch (NoConnectionsException e) {
//            log.error("Caught", e);
        }
        this.connections.clear();
        this.connectionList.clear();
    }

    public String getKey(String host, int port) {
        return host + ":" + port;
    }
}
