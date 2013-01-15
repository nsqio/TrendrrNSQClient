/**
 * 
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import frames.NSQFrame;


/**
 * @author Dustin Norlander
 * @created Jan 14, 2013
 * 
 */
public class NSQHandler extends SimpleChannelUpstreamHandler {

	protected static Log log = LogFactory.getLog(NSQHandler.class);
	
	@Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		NSQFrame frame = (NSQFrame)e.getMessage();
		
		Connection con = (Connection)e.getChannel().getAttachment();
		if (con != null) {
			con.incoming(frame);
		} else {
			log.warn("No connection set for : " + e.getChannel());
			//TODO: should we kill the channel?
		}
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
    	Connection con = (Connection)e.getChannel().getAttachment();
		if (con != null) {
			con.disconnected();
		} else {
			log.warn("No connection set for : " + e.getChannel());
		}
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception {
    	log.warn("Caught", e.getCause());
        e.getChannel().close();
        
        Connection con = (Connection)e.getChannel().getAttachment();
		if (con != null) {
			con.disconnected();
		} else {
			log.warn("No connection set for : " + e.getChannel());
		}
    }
	
}
