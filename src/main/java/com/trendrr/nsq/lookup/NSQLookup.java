package com.trendrr.nsq.lookup;

import com.trendrr.nsq.ConnectionAddress;

import java.util.List;


/**
 * An interface to the nsq lookup.  We keep this as an interface because it depends on 
 * some json parsing library and we don't want to force a dependency on a specific lib.
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
	 * @return the list of nsq servers where this topic is available
	 */
	public List<ConnectionAddress> lookup(String topic);
}
