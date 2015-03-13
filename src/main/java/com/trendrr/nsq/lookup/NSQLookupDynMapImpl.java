/**
 *
 */
package com.trendrr.nsq.lookup;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.trendrr.nsq.ConnectionAddress;
import com.trendrr.oss.DynMap;


/**
 * Lookup implementation based on trendrr-oss DynMap
 *
 * @author Dustin Norlander
 * @created Jan 23, 2013
 */
public class NSQLookupDynMapImpl implements NSQLookup {
    private static Logger LOGGER = LoggerFactory.getLogger(NSQLookupDynMapImpl.class);

    //By default log errors every five minutes otherwise logs become terribly verbose
    private volatile long errorLoggingInterval = 5 * 60 * 1000;

    private final Set<String> addresses = new HashSet<String>();

    private volatile long lastError = 0;

    public void addAddr(String addr, int port) {
        if (!addr.startsWith("http")) {
            addr = "http://" + addr;
        }
        addr = addr + ":" + port;
        this.addresses.add(addr);
    }

    public List<ConnectionAddress> lookup(String topic) {
        HashMap<String, ConnectionAddress> addresses = new HashMap<String, ConnectionAddress>();

        for (String addr : this.addresses) {
            DynMap mp = DynMap.instance(this.getHTML(addr + "/lookup?topic=" + topic), new DynMap());
            for (DynMap node : mp.getListOrEmpty(DynMap.class, "data.producers")) {
                String host = node.getString("broadcast_address", node.getString("address"));
                String key = host + ":" + node.getInteger("tcp_port");

                addresses.put(key, new ConnectionAddress(host, node.getInteger("tcp_port")));
            }
        }
        return new ArrayList<ConnectionAddress>(addresses.values());
    }

    public String getHTML(String url) {
        URL u;
        HttpURLConnection conn = null;
        BufferedReader rd = null;
        String line;
        String result = "";
        try {
            u = new URL(url);
            conn = (HttpURLConnection) u.openConnection();
            conn.setRequestMethod("GET");
            rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            while ((line = rd.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {
            logException("Caught an exception when trying to get nsq instances from lookup mechanism [url=" + url + "]", e);
        } finally {
            try {
                if (rd != null){
                    rd.close();
                }
            } catch (Exception e) {
                logException("Caught an exception when trying to close buffered reader", e);
            }
            //Make sure to close the HTTP connection as well, otherwise the file descriptors will remain open
            if (conn != null){
                conn.disconnect();
            }
        }
        return result;
    }

    private void logException(String message, Exception e) {
        final long now = System.currentTimeMillis();
        if (now - lastError > errorLoggingInterval) {
            LOGGER.error(message, e);
            lastError = now;
        }
    }

    public long getErrorLoggingInterval() {
        return errorLoggingInterval;
    }

    public void setErrorLoggingInterval(long errorLoggingInterval) {
        this.errorLoggingInterval = errorLoggingInterval;
    }
}
