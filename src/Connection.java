/**
 * 
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import commands.NSQCommand;

import frames.NSQFrame;


/**
 * @author Dustin Norlander
 * @created Jan 14, 2013
 * 
 */
public class Connection {

	protected static Log log = LogFactory.getLog(Connection.class);
	
	Channel channel;
	
	public Connection(Channel channel) {
		this.channel = channel;
		this.channel.setAttachment(this);
	}
	
	
	public void incoming(NSQFrame frame) {
		//incoming message, give back to whoever needs it.
		System.out.println("GOT FRAME! " + frame);
	}
	
	void disconnected() {
		//clean up anything that needs cleaning up.
	}
	
	public void close() {
		try {
			channel.close().await(10000);
		} catch (Exception x) {
			log.error("Caught", x);
		}
		this.disconnected();
	}
	
	public ChannelFuture command(NSQCommand command) {
		return this.channel.write(command);
	}
}
