package com.github.brainlag.nsq.lookup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.brainlag.nsq.ServerAddress;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;

import java.io.IOException;
import java.net.URL;
import java.util.Set;
public class DefaultNSQLookup implements NSQLookup {
    Set<String> addresses = Sets.newHashSet();

    @Override
    public void addLookupAddress(String addr, int port) {
        if (!addr.startsWith("http")) {
            addr = "http://" + addr;
        }
        addr = addr + ":" + port;
        this.addresses.add(addr);
    }

    @Override
    public Set<ServerAddress> lookup(String topic) {
        Set<ServerAddress> addresses = Sets.newHashSet();

        for (String addr : getLookupAddresses()) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readTree(new URL(addr + "/lookup?topic=" + topic));
                LogManager.getLogger(this).debug("Server connection information: " + jsonNode.toString());
                JsonNode producers = jsonNode.get("data").get("producers");
                for (JsonNode node : producers) {
                    String host = node.get("broadcast_address").asText();
                    ServerAddress address = new ServerAddress(host, node.get("tcp_port").asInt());
                    addresses.add(address);
                }
            } catch (IOException e) {
                LogManager.getLogger(this).warn("Unable to connect to address " + addr);
            }
        }
        if (addresses.isEmpty()) {
            LogManager.getLogger(this).warn("Unable to connect to any NSQ Lookup servers, servers tried: " + this.addresses.toString());
        }
        return addresses;
    }

    public Set<String> getLookupAddresses() {
        return addresses;
    }
}
