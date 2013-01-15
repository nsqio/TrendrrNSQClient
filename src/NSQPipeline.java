/**
 * 
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;


/**
 * @author Dustin Norlander
 * @created Jan 14, 2013
 * 
 */
public class NSQPipeline implements ChannelPipelineFactory {

	protected static Log log = LogFactory.getLog(NSQPipeline.class);
	
	public ChannelPipeline getPipeline() throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("codec", new NSQDecoder());

        pipeline.addLast("handler", new NSQHandler());
        
        pipeline.addLast("encoder", new NSQEncoder());
        return pipeline;
    }
	
}
