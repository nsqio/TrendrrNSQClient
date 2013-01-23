package com.trendrr.nsq;
/**
 * 
 */


import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import com.trendrr.oss.DynMap;


/**
 * An interface to the nsq lookup.  We keep this as an interface because it depends on 
 * some json parsing library and we dont want to force a dependancy on a specific lib. 
 * 
 * 
 * @author Dustin Norlander
 * @created Jan 14, 2013
 * 
 */
public interface NSQLookup {
	/**
	 * add an address to a nsq lookup server.
	 * @param addr
	 * @param port
	 */
	public void addAddr(String addr, int port);
	
	/**
	 * Lookup topic addresses
	 * @param topic
	 * @return
	 */
	public List<ConnectionAddress> lookup(String topic);
}
