package com.trendrr.nsq;
/**
 * 
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;



/**
 * @author Dustin Norlander
 * @created Jan 14, 2013
 * 
 */
public class NSQEncoder extends OneToOneEncoder {

	protected static Log log = LogFactory.getLog(NSQEncoder.class);

	/* (non-Javadoc)
	 * @see org.jboss.netty.handler.codec.oneone.OneToOneEncoder#encode(org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.channel.Channel, java.lang.Object)
	 */
	@Override
	protected Object encode(ChannelHandlerContext ctx, Channel channel,
			Object message) throws Exception {
		if (!(message instanceof NSQCommand)) {
			return message;
		}
		
		NSQCommand com = (NSQCommand)message;

		System.out.println("Sending: " + com.getLine());
		ChannelBuffer buf = ChannelBuffers.dynamicBuffer();
		
		buf.writeBytes(com.getLine().getBytes("utf8"));
		for (byte[] data : com.getData()) {
			buf.writeInt(data.length);
			buf.writeBytes(data);
		}
		return buf;
	}
}
