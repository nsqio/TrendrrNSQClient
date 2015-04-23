package com.github.brainlag.nsq.lookup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.brainlag.nsq.ServerAddress;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.net.URL;
import java.util.Set;

public class NSQLookup {
	Set<String> addresses = Sets.newHashSet();

	public void addAddr(String addr, int port) {
		if (!addr.startsWith("http")) {
			addr = "http://" + addr;
		}
		addr = addr + ":" + port;
		this.addresses.add(addr);
	}

	public Set<ServerAddress> lookup(String topic) throws IOException {
		Set<ServerAddress> addresses = Sets.newHashSet();
		
		for (String addr : this.addresses) {
            ObjectMapper mapper = new ObjectMapper();

            JsonNode jsonNode = mapper.readTree(new URL(addr + "/lookup?topic=" + topic));
            JsonNode producers = jsonNode.get("data").get("producers");
            for (JsonNode node : producers) {
                String host = node.get("broadcast_address").asText();
				ServerAddress address = new ServerAddress(host, node.get("tcp_port").asInt());
				addresses.add(address);
			}
		}
		return addresses;
	}
}
