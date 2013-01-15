/**
 * 
 */

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;


/**
 * @author Dustin Norlander
 * @created Jan 14, 2013
 * 
 */
public class NSQLookup {

	protected static Log log = LogFactory.getLog(NSQLookup.class);
	
	Set<String> addresses = new HashSet<String> ();
	AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
	
	public void addAddr(String addr, int port) {
		if (!addr.startsWith("http")) {
			addr = "http://" + addr;
		}
		addr = addr + ":" + port;
		this.addresses.add(addr);
	}
	
	//TODO: return a parsed response
	public String lookup(String topic) {
		for (String str: addresses) {
			try {
			    Future<Response> f = asyncHttpClient.prepareGet(str + "/lookup?topic=test").execute();
			    Response r = f.get();
			    //Return the first response we get.
			    // is this correct? do we need to union results from all?
			    return r.getResponseBody();
			} catch (Exception x) {
				log.error("Caught", x);
			}
		}
		return null;
	}
	
	
	
	
	
}
