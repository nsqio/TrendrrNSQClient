package com.trendrr.nsq.lookup;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.Sets;
import com.trendrr.nsq.ConnectionAddress;

public class NSQLookup {
	Set<String> addresses = Sets.newHashSet();

	public void addAddr(String addr, int port) {
		if (!addr.startsWith("http")) {
			addr = "http://" + addr;
		}
		addr = addr + ":" + port;
		this.addresses.add(addr);
	}
	
	public List<ConnectionAddress> lookup(String topic) throws IOException {
		HashMap<String, ConnectionAddress> addresses = new HashMap<>();
		
		for (String addr : this.addresses) {
            ObjectMapper mapper = new ObjectMapper();

            JsonNode jsonNode = mapper.readTree(new URL(addr + "/lookup?topic=" + topic));
            JsonNode producers = jsonNode.get("data").get("producers");
            for (JsonNode node : producers) {
				String host = node.get("address").asText();
				String key =  host + ":" + node.get("tcp_port").asText();
				ConnectionAddress address = new ConnectionAddress();
				address.setHost(host);
				address.setPort(node.get("tcp_port").asInt());
				addresses.put(key, address);
			}
		}
		return new ArrayList<>(addresses.values());
	}
}
