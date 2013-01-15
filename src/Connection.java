/**
 * 
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import commands.NSQCommand;

import frames.NSQFrame;
import frames.ResponseFrame;


/**
 * @author Dustin Norlander
 * @created Jan 14, 2013
 * 
 */
public class Connection {

	protected static Log log = LogFactory.getLog(Connection.class);
	
	Channel channel;
	int heartbeats = 0;
	public Connection(Channel channel) {
		this.channel = channel;
		this.channel.setAttachment(this);
	}
	
	
	public void incoming(NSQFrame frame) {
		if (frame instanceof ResponseFrame) {
			if ("_heartbeat_".equals(((ResponseFrame) frame).getMessage())) {
				this.heartbeat();
				return;
			}
			
			
		}
		
		//incoming message, give back to whoever needs it.
		System.out.println("GOT FRAME! " + frame);
	}
	
	
	void heartbeat() {
		System.out.println("HEARTBEAT!");
		this.heartbeats++;
		//send NOP here.
		this.command(NSQCommand.instance("NOP"));
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
