package com.trendrr.nsq.netty;
/**
 *
 */

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.trendrr.nsq.NSQCommand;



/**
 * @author Dustin Norlander
 * @created Jan 14, 2013
 *
 */
public class NSQEncoder extends OneToOneEncoder {

    protected static Logger log = LoggerFactory.getLogger(NSQEncoder.class);

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

        ChannelBuffer buf = ChannelBuffers.dynamicBuffer();
        buf.writeBytes(com.getLine().getBytes("utf8"));

        //for MPUB messages.
        if (com.getData().size() > 1) {
            //write total bodysize and message size
            int bodySize = 4; //4 for total messages int.
            for (byte[] data : com.getData()) {
                bodySize += 4; //message size
                bodySize += data.length;
            }
            buf.writeInt(bodySize);
            buf.writeInt(com.getData().size());
        }

        for (byte[] data : com.getData()) {
            buf.writeInt(data.length);
            buf.writeBytes(data);
        }
        return buf;
    }
}
