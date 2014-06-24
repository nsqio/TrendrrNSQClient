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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.trendrr.nsq.ConnectionAddress;
import com.trendrr.nsq.NSQLookup;

/**
 * Lookup implementation based on trendrr-oss DynMap
 *
 *
 * @author Dustin Norlander
 * @author Max Alexejev
 * @created Jan 23, 2013
 *
 */
public class NSQLookupImpl implements NSQLookup {

    protected static Logger log = LoggerFactory.getLogger(NSQLookupImpl.class);

    Set<String> addresses = new HashSet<String>();

    @Override
    public void addAddr(String addr, int port) {
        if (!addr.startsWith("http")) {
            addr = "http://" + addr;
        }
        addr = addr + ':' + port;
        this.addresses.add(addr);
    }

    @Override
    public List<ConnectionAddress> lookup(String topic) {
        JsonParser jsonParser = new JsonParser();
        HashMap<String, ConnectionAddress> addresses = new HashMap<String, ConnectionAddress>();

        for (String addr : this.addresses) {
            try {
                JsonObject json = jsonParser.parse(this.getHTML(addr + "/lookup?topic=" + topic)).getAsJsonObject();
                for (JsonElement elem : json.get("data").getAsJsonObject().get("producers").getAsJsonArray()) {
                    JsonObject node = elem.getAsJsonObject();
                    String host = node.get("broadcast_address").getAsString();
                    String key = host + ":" + node.get("tcp_port").getAsInt();
                    ConnectionAddress address = new ConnectionAddress();
                    address.setHost(host);
                    address.setPort(node.get("tcp_port").getAsInt());
                    addresses.put(key, address);
                }
            } catch (Exception e) {
                log.warn("Caught", e);
                // continue;
            }
        }
        return new ArrayList<ConnectionAddress>(addresses.values());
    }

    public String getHTML(String url) throws Exception {
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

        } finally {
            try {
                if (rd != null) {
                    rd.close();
                }
            } catch (Exception e) {
                log.error("Caught", e);
            }

            // Release memory and underlying resources on the HttpURLConnection
            // otherwise we may run out of file descriptors and leak memory
            if (conn != null) {
                conn.disconnect();
            }
        }
        return result;
    }

}
