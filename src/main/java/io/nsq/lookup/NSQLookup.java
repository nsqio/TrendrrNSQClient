package io.nsq.lookup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import io.nsq.ServerAddress;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

	public List<ServerAddress> lookup(String topic) throws IOException {
		HashMap<String, ServerAddress> addresses = new HashMap<>();
		
		for (String addr : this.addresses) {
            ObjectMapper mapper = new ObjectMapper();

            JsonNode jsonNode = mapper.readTree(new URL(addr + "/lookup?topic=" + topic));
            JsonNode producers = jsonNode.get("data").get("producers");
            for (JsonNode node : producers) {
                String host = node.get("broadcast_address").asText();
                String key =  host + ":" + node.get("tcp_port").asText();
				ServerAddress address = new ServerAddress(host, node.get("tcp_port").asInt());
				addresses.put(key, address);
			}
		}
		return new ArrayList<>(addresses.values());
	}
}
