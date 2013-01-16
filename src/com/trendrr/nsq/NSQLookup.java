package com.trendrr.nsq;
/**
 * 
 */


import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import com.trendrr.oss.DynMap;


/**
 * @author Dustin Norlander
 * @created Jan 14, 2013
 * 
 */
public class NSQLookup {

	protected static Log log = LogFactory.getLog(NSQLookup.class);
	
	Set<String> addresses = new HashSet<String> ();
	AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
	
	public NSQLookup() {
		
	}
	
	public void addAddr(String addr, int port) {
		if (!addr.startsWith("http")) {
			addr = "http://" + addr;
		}
		addr = addr + ":" + port;
		this.addresses.add(addr);
	}
	
	public DynMap lookup(String topic) {
		return get("/lookup?topic=" + topic);
	}
	
	protected DynMap get(String endpoint) {
		for (String str: addresses) {
			try {
			    Future<Response> f = asyncHttpClient.prepareGet(str + endpoint).execute();
			    Response r = f.get();
			    //Return the first response we get.
			    // is this correct? do we need to union results from all?
			    return DynMap.instance(r.getResponseBody());
			} catch (Exception x) {
				log.error("Caught", x);
			}
		}
		return null;
	}
	
	public DynMap nodes() {
		return this.get("/nodes");
	}
}
